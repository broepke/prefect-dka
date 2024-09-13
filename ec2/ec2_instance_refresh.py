"""Bash operator to refresh AWS EC2 AutoScaling Group on Schedule"""
from prefect import flow
from prefect_shell import ShellOperation


@flow(name="Refresh EC2 Instances", retries=3, retry_delay_seconds=30)
def refresh_instances():
    """
    Refresh EC2 Instances On a Schedule Set in Prefect
    """
    ShellOperation(
        commands=[
            "aws autoscaling start-instance-refresh --auto-scaling-group-name prefect-workers-asg"
        ],
    ).run()


if __name__ == "__main__":
    refresh_instances()
