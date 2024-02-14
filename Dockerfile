# We're using the latest version of Prefect with Python 3.10
FROM prefecthq/prefect:2-python3.10

# Add our requirements.txt file to the image and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir

# Run our flow script when the container starts
CMD ["/bin/sh","-c","prefect worker start --pool my-ecs-pool --type ecs"]