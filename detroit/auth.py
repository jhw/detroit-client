import boto3
import os
import re

def hungarorise(text):
    return "".join([tok.capitalize()
                    for tok in re.split("\\-|\\_", text)])

def fetch_outputs(cf, stack_name):
    outputs = {}
    for stack in cf.describe_stacks()["Stacks"]:
        if (stack["StackName"] == stack_name and
            "Outputs" in stack):
            for output in stack["Outputs"]:
                outputs[output["OutputKey"]] = output["OutputValue"]
    return outputs

def fetch_token(stack_name, email, password):                        
    cf = boto3.client("cloudformation")
    outputs = fetch_outputs(cf, stack_name)
    userpool_key = hungarorise("app-user-pool")
    if userpool_key not in outputs:
        raise RuntimeError("userpool not found")
    userpool = outputs[userpool_key]
    client_key = hungarorise("app-user-pool-client")
    if client_key not in outputs:
        raise RuntimeError("client not found")
    client = outputs[client_key]
    cg = boto3.client("cognito-idp")
    auth_resp = cg.admin_initiate_auth(UserPoolId=userpool,
                                       ClientId=client,
                                       AuthFlow='ADMIN_NO_SRP_AUTH',
                                       AuthParameters={"USERNAME": email,
                                                       "PASSWORD": password})
    return auth_resp["AuthenticationResult"]["IdToken"]


if __name__ == "__main__":
    pass
