from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import DataSecurityMode

# Creating a workspaceclient to connect the workspace. See documentation here: https://docs.databricks.com/en/dev-tools/sdk-python.html#authenticate-the-databricks-sdk-for-python-with-your-databricks-account-or-workspace 
w = WorkspaceClient()

# List all the jobs on the workspace
for c in w.jobs.list(expand_tasks=True):
    #Geting all the job clusters
    job_clusters = c.settings.job_clusters

    for job_cluster in job_clusters:
        # Filtering new job clusters if it is created with SINGLE USER data security mode
        if job_cluster.new_cluster.data_security_mode in [DataSecurityMode.LEGACY_SINGLE_USER, DataSecurityMode.SINGLE_USER]:
            print('----')
            print(c)
            print('----')
            break