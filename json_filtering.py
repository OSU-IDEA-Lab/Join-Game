import json

def extract_required_fields(json_data):
    # Define the fields we need to extract
    required_fields = ['id', 'title', 'author_name', 'language', 'original_publication_date', 'description']

    # Extract data from the JSON object
    filtered_data = {
        "id": json_data.get("id", ""),
        "title": json_data.get("title", ""),
        "author_name": json_data.get("author_name", ""),
        "language": json_data.get("language", ""),
        "original_publication_date": json_data.get("original_publication_date", ""),
        "description": json_data.get("description", "").replace("<br /><br />", "\n\n")  # Convert HTML to newlines
    }

    # Check and only include required fields
    return {key: value for key, value in filtered_data.items() if key in required_fields}

def read_and_filter_json(input_file, output_file):
    # Initialize an empty list to hold all filtered data
    all_filtered_data = []

    # Open and read the JSON file
    with open(input_file, 'r') as file:
        for line in file:
            try:
                # Load JSON data from each line assuming each line is a separate JSON object
                json_data = json.loads(line)
                # Extract the required information
                filtered_data = extract_required_fields(json_data)
                all_filtered_data.append(filtered_data)
            except ValueError as e:
                # Handle the case where JSON decoding fails
                print("Failed to decode JSON: ", e)
                continue

    # Save the filtered data to a new JSON file
    with open(output_file, 'w') as file:
        json.dump(all_filtered_data, file, indent=4)

def main():
    input_file = '/data/mettas/data/books.json'  # Update this path
    output_file = '/data/mettas/data/filtered_books.json'  # Update this path
    read_and_filter_json(input_file, output_file)

if __name__ == "__main__":
    main()
