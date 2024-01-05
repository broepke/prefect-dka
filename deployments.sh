prefect deployment build ec2/ec2_instance_refresh.py:refresh_instances \
    --name refresh-instances-github \
    --storage-block github/github-repo \
    --work-queue default  \
    --apply \
    --skip-upload

prefect deployment build deadpool/deadpool.py:dead_pool_status_check \
    --name deadpool-github \
    --storage-block github/github-repo \
    --work-queue default  \
    --apply \
    --skip-upload

prefect deployment build deadpool/deadpool_nndb.py:deadpool_nndb_date_updates \
    --name deadpool-github \
    --storage-block github/github-repo \
    --work-queue default  \
    --apply \
    --skip-upload
