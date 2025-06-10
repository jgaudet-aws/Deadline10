import os
import shutil
import tempfile
import json
import deadline.client.ui.job_bundle_submitter as cloud_submitter
import deadline.client.ui.dialogs.submit_job_to_deadline_dialog
import qtpy.QtWidgets

from openjd.model.v2023_09 import *
from typing import Any
from unittest import mock

from Deadline.Applications import DeadlineApplicationManager
from Deadline.Scripting import ClientUtils, MonitorUtils, RepositoryUtils

MAX_STEP_NAME_CHARS = 64
MAX_STEP_DESCRIPTION_CHARS = 2048

# Mapping from Deadline10 plugin names to Deadline Cloud conda packages
PLUGIN_CONDA_MAPPING = {
    "DraftTileAssembler": ["draft", "six"],
    "MayaBatch": ["maya>=2024", "maya-mtoa>=2024"],
    "MayaCmd": ["maya>=2024", "maya-mtoa>=2024"],
    "Blender": ["blender"],
}

def __main__():
    """
    Monitor script that takes a selected Job and exports it to Deadline Cloud.

    Requires Deadline 10.4.1, or the Deadline Cloud python package available in the sys.path.
    """
    selected_jobs = MonitorUtils.GetSelectedJobs()
    
    if selected_jobs:
        # Creates a Deadline Cloud submission bundle based on the selected jobs
        bundle_dir = create_cloud_submission_bundle(selected_jobs)


        # HACK: We need to monkeypatch qtpy's QDialog constructor
        #       to translate PySide 'f' kwarg into PyQt's 'flags' kwarg.
        real_qd_init = qtpy.QtWidgets.QDialog.__init__
        def patched_qdialog_init(self, *args, **kwargs):
            if 'f' in kwargs:
                kwargs['flags'] = kwargs['f']
                del kwargs['f']
            return real_qd_init(self, *args, **kwargs)

        # HACK: Suppress the Host Requirements tab because it seems to not like our PyQt bindings.
        real_sjtd_init = deadline.client.ui.dialogs.submit_job_to_deadline_dialog.SubmitJobToDeadlineDialog.__init__
        def patched_sjtd_init(self, *args, **kwargs):
            # Force the host requirements tab to be hidden; something about its implementation doesn't work w/ D10's PyQt bindings
            if 'show_host_requirements_tab' in kwargs:
                kwargs['show_host_requirements_tab'] = False
            return real_sjtd_init(self, *args, **kwargs)

        try:
            with mock.patch('qtpy.QtWidgets.QDialog.__init__', new=patched_qdialog_init):
                with mock.patch('deadline.client.ui.dialogs.submit_job_to_deadline_dialog.SubmitJobToDeadlineDialog.__init__', new=patched_sjtd_init):
                    submitter = cloud_submitter.show_job_bundle_submitter(
                        input_job_bundle_dir=bundle_dir,
                        submitter_name="Export to Deadline Cloud"
                    )

                    # HACK: Hide the job settings tab since we auto-populated all of those values
                    job_settings_index = submitter.tabs.indexOf(submitter.job_settings_tab)
                    if job_settings_index > 0:
                        submitter.tabs.removeTab(job_settings_index)
                    
                    # Set this as a modal window so it doesn't get lost
                    submitter.setModal(True)
                    submitter.hide()

                    print("Displaying Deadline Cloud submission dialog...")
                    submitter.exec()
        except:
            import traceback
            print("Error submitting job bundle to Deadline Cloud.")
            print(traceback.format_exc())


def create_cloud_submission_bundle(d10_jobs) -> str:
    print(f"Creating Deadline Cloud job bundle...")

    # Topologically sort jobs such that they are guaranteed to appear in dependency order
    # Raises an error if the list of given jobs is missing dependencies
    d10_jobs = _topologically_sort_jobs(d10_jobs)

    # TODO: The spot where this directory is created actually kinda matters.
    #       If it's near the root dir, (e.g., `/tmp`), this can implicitly create a Job Attachment AssetRoot of `/`,
    #       which will mess with all PathMapping at render time (all paths start with `/` and will get that root swapped in).
    #       We really should try and force NON-root asset roots on the DeadlineCloud side.
    bundle_root_dir = tempfile.mkdtemp(prefix='d10_cloud_export', dir=ClientUtils.GetDeadlineTempPath())

    print(f"\tRoot Directory: {bundle_root_dir}")

    # Step 1: Copy over plugin into job bundle
    rel_plugin_dir = _copy_plugin_files(d10_jobs, bundle_root_dir)

    # Step 2: Copy Job/Plugin info files into job bundle
    rel_job_info_files_by_d10_id = _copy_job_info_files(d10_jobs, bundle_root_dir)

    # Step 3: Copy any auxiliary files (if present) into job bundle
    aux_files_by_d10_id = _copy_job_aux_files(d10_jobs, bundle_root_dir)

    # Step 4: Derive required conda packages from mapping
    conda_packages = _get_required_conda_packages(d10_jobs)

    # Step 5: Add known Job Attachments from Deadline 10 jobs
    _add_asset_references(d10_jobs, bundle_root_dir)

    # Step 6: Creates Job Template w/ parameters based on given D10 jobs
    cloud_job_template = _create_job_template(rel_plugin_dir, rel_job_info_files_by_d10_id, conda_packages)

    # Step 7: Add one OJD Step to the template for each D10 job
    d10_ids_to_step_names = {}
    for d10_job in d10_jobs:
        d10_job_assets = aux_files_by_d10_id[d10_job.ID]
        step_json = create_step_from_job(d10_job, d10_job_assets, d10_ids_to_step_names)
        d10_ids_to_step_names[d10_job.ID] = step_json["name"]

        cloud_job_template["steps"].append(step_json)

    # Write out the template to the job bundle directory
    with open(os.path.join(bundle_root_dir, "template.yaml"), "w") as f:
        json.dump(cloud_job_template, f, indent=4)

    return bundle_root_dir

def _topologically_sort_jobs(d10_jobs):
    """
    Sorts given D10 jobs in dependency order.

    Raises a RuntimeError if the list of jobs is not a complete dependency closure.
    """
    checked_jobs = set()
    sorted_jobs = []

    jobs_by_id = {}
    for job in d10_jobs:
        jobs_by_id[job.ID] = job

    while len(jobs_by_id) > 0:
        added_jobs = []
        for job_id, job in jobs_by_id.items():
            if len(job.JobDependencies) == 0 or all(dep.JobID in checked_jobs for dep in job.JobDependencies):
                sorted_jobs.append(job)
                checked_jobs.add(job_id)
                added_jobs.append(job_id)

        if len(added_jobs) == 0:
            raise RuntimeError("Missing parts of dependency closure; please select all depedendent Jobs")
        
        for job_id in added_jobs:
            del jobs_by_id[job_id]

    return sorted_jobs


def _copy_plugin_files(d10_jobs, bundle_root_dir: str):
    """
    Pulls all the plugin files required for the given D10 jobs, and copies them into the bundle directory.
    """

    bundle_plugins_basedir = os.path.join(bundle_root_dir, "assets", "plugins")
    
    os.makedirs(bundle_plugins_basedir, exist_ok=True)

    copied_plugins = set()

    for d10_job in d10_jobs:
        plugin_name = d10_job.PluginName

        if plugin_name in copied_plugins:
            continue

        # Copy the plugin to the submission bundle
        print(f"\tCopying files for {plugin_name} plugin...")

        plugin_dir = RepositoryUtils.GetPluginDirectory(plugin_name)
        
        bundle_plugin_dir = os.path.join(bundle_plugins_basedir, plugin_name)
        shutil.copytree(plugin_dir, bundle_plugin_dir)

        # Persist the Plugin's current config as a *.dlinit file alongside the bundled plugin
        print(f"\tPersisting configuration for {plugin_name} plugin...")
        dl_init_file = os.path.join(bundle_plugin_dir, f'{plugin_name}.dlinit')
        plugin_config = RepositoryUtils.GetPluginConfig(plugin_name)
        config_keys = plugin_config.GetConfigKeys()
        with open(dl_init_file, "w") as f:
            for key in config_keys:
                value = plugin_config.GetConfigEntry(key)
                f.write(f'{key}={value}\n')

        copied_plugins.add(plugin_name)

    return os.path.relpath(bundle_plugins_basedir, bundle_root_dir)


def _get_job_info_file_from_job(d10_job):
    file_lines =[f"{kvp.Key}={kvp.Value}" for kvp in d10_job.JobInfoFromJob(d10_job, False)]

    return '\n'.join(file_lines)


def _get_plugin_info_file_from_job(d10_job):
    file_lines = [f"{kvp.Key}={kvp.Value}" for kvp in d10_job.PluginInfoDictionary]

    return '\n'.join(file_lines)


def _copy_job_info_files(d10_jobs, bundle_root_dir: str) -> dict[str, tuple[str, str]]:
    """
    Exports Job/Plugin info files for the given list of jobs into the bundle directory.
    """
    bundle_assets_dir = os.path.join( bundle_root_dir, "assets" )
    os.makedirs(bundle_assets_dir, exist_ok=True)

    job_info_files = {}

    print(f"\tCopying job/plugin info files for {len(d10_jobs)} jobs...")
    for d10_job in d10_jobs:
        # Create the *.job INI files for the job
        job_info_path = os.path.join(bundle_assets_dir, f"{d10_job.ID}_jobInfo.job")
        with open(job_info_path, "w") as f:
            f.write(_get_job_info_file_from_job(d10_job))

        plugin_info_path = os.path.join(bundle_assets_dir, f"{d10_job.ID}_pluginInfo.job")
        with open(plugin_info_path, "w") as f:
            f.write(_get_plugin_info_file_from_job(d10_job))

        job_info_files[d10_job.ID] = (
            os.path.relpath(job_info_path, bundle_root_dir),
            os.path.relpath(plugin_info_path, bundle_root_dir)
        )

    return job_info_files


def _copy_job_aux_files(d10_jobs, bundle_root_dir: str) -> dict[str, list[str]]:
    """
    Copies all auxiliary files for the given list of d10 jobs into the bundle directory.
    """

    bundle_jobs_basedir = os.path.join( bundle_root_dir, "assets", "jobs" )
    dataController = DeadlineApplicationManager.GetBaseInstance().DataController

    aux_files_by_d10_id = {}

    print(f"\tCopying auxiliary files for {len(d10_jobs)} jobs...")
    for d10_job in d10_jobs:
        job_asset_dir = os.path.join(bundle_jobs_basedir, d10_job.ID)
        os.makedirs(job_asset_dir, exist_ok=True)

        # Download Auxiliary files to the same folder
        dataController.JobStorage.DownloadAuxiliaryFiles(d10_job, job_asset_dir, False, "", False)
        
        bundled_aux_files = list(d10_job.AuxiliarySubmissionFileNames)

        aux_files_by_d10_id[d10_job.ID] = bundled_aux_files

        print(f"\t\tCopied {len(bundled_aux_files)} aux files for job '{d10_job.ID}'")

    return aux_files_by_d10_id


def _get_required_conda_packages(d10_jobs) -> list[str]:
    """
    Returns a set of known required conda packages derived from the given list of D10 jobs.
    """
    conda_packages = {'deadline-10-runtime'}
    for d10_job in d10_jobs:
        for package in PLUGIN_CONDA_MAPPING[d10_job.PluginName]:
            conda_packages.add(package)

    return list(conda_packages)


def _add_asset_references(d10_jobs, bundle_root_dir):
    output_dirs = set()
    for d10_job in d10_jobs:
        for dir in d10_job.OutputDirectories:
            output_dirs.add(dir)

    print("\tCalculating Asset References...")
    asset_refs = {
        'assetReferences':{
            # TODO: We could try to populate JA inputs from AWS Asset Portal metadata, if asset instrospection is turned on.
            #       We could also have a known list of PluginInfo keys per plugin to pull any known input files/directory into here.
            'inputs' : {
                'filenames': [],
                'directories': [],
            },
            'outputs' : {
                'directories': list(output_dirs)
            }
        }
    }

    print('\tWriting Asset References...')
    ja_file = os.path.join(bundle_root_dir, 'asset_references.json')
    with open(ja_file, 'w') as f:
        json.dump(asset_refs, f)


def _create_job_template(rel_plugin_dir: str, job_files: dict[str, tuple[str, str]], required_conda_packages: list[str]) -> dict[str, Any]:

    submission_info_files = []
    for job_id, file_tuple in job_files.items():
        job_info, plugin_info = file_tuple

        submission_info_files.append({
            "name": f"JobInfo_{job_id}",
            "type": "PATH",
            "default": job_info,
            "description": f"Job Info file for exported Deadline 10 Job with ID '{job_id}'",
            "objectType": "FILE",
            "dataFlow": "IN"
        })
        submission_info_files.append({
            "name": f"PluginInfo_{job_id}",
            "type": "PATH",
            "default": plugin_info,
            "description": f"Plugin Info file for exported Deadline 10 Job with ID '{job_id}'",
            "objectType": "FILE",
            "dataFlow": "IN"
        })
        submission_info_files.append({
            "name": f"AuxFiles_{job_id}",
            "type": "PATH",
            "default": f"assets/jobs/{job_id}",
            "description": f"Auxiliary files for exported Deadline 10 Job with ID '{job_id}'",
            "objectType": "DIRECTORY",
            "dataFlow": "IN"
        })

    job_template = {
        "specificationVersion": "jobtemplate-2023-09",
        "name": "Deadline 10 Export Jobs",
        "description": "",
        "parameterDefinitions": [
            {
                "name": "D10PluginDirectory",
                "type": "PATH",
                "default": rel_plugin_dir,
                "description": "Directory containing the Deadline 10 plugins",
                "objectType": "DIRECTORY",
                "dataFlow": "IN"
            },
            {
                "name": "CondaPackages",
                "type": "STRING",
                "default": " ".join(required_conda_packages)
            },
            *submission_info_files
        ],
        "steps": [],
    }

    return job_template


def _get_step_name_from_d10_job(d10_job):
    id_postfix = f" ({d10_job.ID})"

    if len(d10_job.Name) + len(id_postfix) > MAX_STEP_NAME_CHARS:
        id_postfix = "..." + id_postfix
        return d10_job.Name[:MAX_STEP_NAME_CHARS - len(id_postfix)] + id_postfix
    else:
        return d10_job.Name + id_postfix


def create_step_from_job(d10_job, aux_filenames: list[str], d10_ids_to_step_names: dict[str, str]) -> dict[str, Any]:
    dep_names = [d10_ids_to_step_names[d10_dep.JobID] for d10_dep in d10_job.JobDependencies]

    relative_aux_files = ["{{Param.AuxFiles_%s}}/%s" % (d10_job.ID, aux_filename) for aux_filename in aux_filenames]

    step_json = {
        "name": _get_step_name_from_d10_job(d10_job),
        "description": f"{d10_job.Name}: {d10_job.Comment}"[:MAX_STEP_DESCRIPTION_CHARS],
        # TODO: Populate host requirements
        # "hostRequirements": {}, 
        "parameterSpace":{
            "taskParameterDefinitions":[
                {
                    "name": "TaskID",
                    "type": "INT",
                    "range": f"0-{d10_job.TaskCount - 1}"
                }
            ]
        },
        "script" : {
            "actions": {
                "onRun": {
                    "command": "bash",
                    "args": [
                        '{{Task.File.RunD10Task}}',
                        '{{Param.JobInfo_%s}}' % d10_job.ID,
                        '{{Param.PluginInfo_%s}}' % d10_job.ID,
                        *relative_aux_files
                    ]
                }
            },
            "embeddedFiles": [
                {
                    "name": "RunD10Task",
                    "type": "TEXT",
                    "filename": f"render_{d10_job.ID}.sh",
                    "data": D10_BASH_SCRIPT
                }
            ]
        },
    }

    if dep_names:
        step_json["dependencies"] = [
            {"dependsOn": dep_name} for dep_name in dep_names
        ]

    return step_json


D10_BASH_SCRIPT = """
# Configure the task to fail if any individual command fails.
set -xeuo pipefail

d10_repo_settings=`pwd`/repo_settings.json

# Translate the Deadline Cloud path mapping format to D10 format
# This also ensures there is a single trailing slash in the source/destination paths
jq '{ MappedOSPath: .path_mapping_rules | map({
        Path: (.source_path|rtrimstr("/") + "/"),
        LinuxPath: (.destination_path|rtrimstr("/") + "/"),
        WindowsPath: (.destination_path|rtrimstr("/") + "/"),
        MacPath: (.destination_path|rtrimstr("/") + "/"),
        CaseSensitive: true,
        RegularExpression: false,
        MapPrefixOnly: true })}' {{Session.PathMappingRulesFile}} > "$d10_repo_settings"

# Job and Plugin info should be the first two params, the rest should be aux files
job_info="$1"
shift
plugin_info="$1"
shift

deadlinecommand RenderJob \
        "$job_info" \
        "$plugin_info" \
        "$@" \
        --standalone \
        --taskid {{Task.Param.TaskID}} \
        --plugindir "{{Param.D10PluginDirectory}}" \
        --reposettings "$d10_repo_settings"
"""
