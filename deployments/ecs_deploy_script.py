# Your default Docker build namespace has been set to '222975130657.dkr.ecr.us-east-1.amazonaws.com'.

# To build and push a Docker image to your newly created repository, use 'prefect-flows' as your image name:

from prefect import flow                               
from prefect.deployments import DeploymentImage        
                                                       
                                                       
@flow(log_prints=True)                                 
def my_flow(name: str = "world"):                      
    print(f"Hello {name}! I'm a flow running on ECS!") 
                                                       
                                                       
if __name__ == "__main__":                             
    my_flow.deploy(                                    
        name="my-deployment",                          
        work_pool_name="dka-ecs-pool",                 
        image=DeploymentImage(                         
            name="prefect-flows:latest",               
            platform="linux/amd64",                    
        )                                              
    )  