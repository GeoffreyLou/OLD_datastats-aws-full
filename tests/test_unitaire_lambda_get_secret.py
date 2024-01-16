import json
import boto3
import os
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    # TODO implement
    
    try:
    
        print('test')
        
        def get_db_secret():
    
            secret_name = os.environ['SECRET_NAME']
            region_name = os.environ['REGION']
        
            # Create a Secrets Manager client
            session = boto3.session.Session()
            client = session.client(
                service_name='secretsmanager',
                region_name=region_name
            )
        
            try:
                get_secret_value_response = client.get_secret_value(
                    SecretId=secret_name
                )
            except ClientError as e:
                raise e
        
            secret = get_secret_value_response['SecretString']
            secret_dict = json.loads(secret)
        
            # Retrieve specific variables (e.g., username and password)
            user = secret_dict.get('username')
        
            return user
            
        user = get_db_secret()
        
        print(user)
        
    
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("Le secret n'a pas été trouvé.")
        else:
            print(f"Erreur Secrets Manager: {str(e)}")
        raise e
    except Exception as e:
        print(f"Erreur inattendue: {str(e)}")
        raise e
