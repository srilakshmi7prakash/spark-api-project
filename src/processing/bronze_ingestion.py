# //'w' refers to 'write' if it was 'a' it means append
import json 

def write_bronze(bronze_path,data):
    with open(bronze_path,"w") as f :
        json.dump(data,f)
    print(f"successfully saved raw data to {bronze_path}")


    