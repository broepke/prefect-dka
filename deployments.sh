prefect deployment build ec2/ec2_instance_refresh.py:refresh_instances \
    --name refresh-instances-github \
    --storage-block github/github-repo \
    --work-queue default  \
    --apply \
    --skip-upload