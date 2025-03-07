import os
import shutil

def delete_unwanted_directories(base_path, allowed_dirs):
    # List all items in the base directory
    for item in os.listdir(base_path):
        item_path = os.path.join(base_path, item)

        # Check if the item is a directory
        if os.path.isdir(item_path):
            # If the directory name is not in the allowed list, delete it
            if item not in allowed_dirs:
                print(f"Deleting directory: {item_path}")
                shutil.rmtree(item_path)


# Define the base directory and the allowed directory names
base_directory = "./data/workmem_500MB/2R/"  # Replace with the path to your directory
allowed_directories = ['HashJoin_results', 'MergeJoin_results', 'SMS_results']

# Call the function
for query in ['Q9', 'Q10', 'Q11', 'Q12', 'Q15']:
    path = base_directory + query
    delete_unwanted_directories(path, allowed_directories)
