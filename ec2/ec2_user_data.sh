#!/bin/bash

# Setup the PATH variable
export PATH=~/.local/bin:$PATH
cd /home

# Create the "prefect" user and set up their environment
new_username="prefect"
password=$(aws secretsmanager get-secret-value --secret-id prefect-ec2-user-pw \
--query 'SecretString' | jq -r '. | fromjson | .key')
sudo useradd -m -s /bin/bash $new_username
echo "$new_username:$password" | chpasswd

# Set up the "prefect" user's environment and create an alias
sudo -u prefect -i bash -c 'echo "alias python=python3" >> ~/.bashrc'

# Reload the "prefect" user's environment
sudo -u prefect -i bash -c "source ~/.bashrc"

# Install PIP and Python under the "prefect" user
sudo -u prefect -i bash -c "curl -O https://bootstrap.pypa.io/get-pip.py"
sudo -u prefect -i bash -c "python3 /home/prefect/get-pip.py --user"
sudo -u prefect -i bash -c "sudo yum install -y python3-devel"
sudo -u prefect -i bash -c "sudo yum install -y gcc"
sudo -u prefect -i bash -c "sudo yum install -y git"

# Install GIT for all users
sudo yum install -y git

# Install Python Required Packages under the "prefect" user
sudo -u prefect -i bash -c "pip install --user \
    asyncpg \
    s3fs \
    prefect \
    prefect-snowflake \
    prefect-shell \
    prefect-slack \
    prefect-github \
    prefect-aws \
    pandas \
    bs4 \
    tldextract \
    pymongo \
    python-whois \
    fuzzywuzzy \
    python-Levenshtein \
    snowflake-connector-python[pandas]"

# Reload environment variables to reflect changes
source ~/.bashrc

# Login to Prefect Cloud via Key from Parameter Store
api_key=$(aws secretsmanager get-secret-value --secret-id prefect-cloud-key \
--query 'SecretString' | jq -r '. | fromjson | .key')
sudo -u prefect -i bash -c "prefect cloud login --key $api_key \
--workspace dka/data-knows-all"

# Create and configure the Prefect Agent service
cd /etc/systemd/system
cat <<EOF | sudo tee /etc/systemd/system/prefect-agent.service
[Unit]
Description=Prefect Agent

[Service]
User=prefect
WorkingDirectory=/home/prefect
ExecStart=/home/prefect/.local/bin/prefect agent start --work-queue "default"
Restart=always

[Install]
WantedBy=multi-user.target
EOF

# Enable and start the service
sudo systemctl daemon-reload
sudo systemctl enable prefect-agent
sudo systemctl start prefect-agent

# Install DataDog
export DD_API_KEY=$(aws secretsmanager get-secret-value \
    --secret-id DataDog \
    --query 'SecretString' --output text) 
export DD_SITE="datadoghq.com"
export DD_APM_INSTRUMENTATION_ENABLED=host
sudo DD_API_KEY="$DD_API_KEY" DD_SITE="$DD_SITE" bash -c "$(curl -L https://s3.amazonaws.com/dd-agent/scripts/install_script_agent7.sh)"