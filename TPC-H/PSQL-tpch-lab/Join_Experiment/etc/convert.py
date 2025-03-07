# Path to the input file
data_path = 'data/data/'

algorithms = ['NestLoop/', 'HashJoin(10MB)/', 'MergeJoin(10MB)/', 'RippleJoin(10MB)/',
           'ORL(N500_MIN500_MAX5K)/', 'OSL(L100_N300_M5000)/', 'SMS(10MB)/']
#queries = ['2R/Q9/', '2R/Q10/', '2R/Q11/', '2R/Q12/', '2R/Q15/', '3R/Q8/', '3R/Q9/']
queries = ['3R/Q9/']

# Initializing the values
start = 2000
increment = 2000
current_value = start
values = set()
count = 0
# Generating the list until the current value exceeds 50,000,000
while current_value <= 50000000:
    values.add(str(current_value))
    current_value += increment
    #if current_value > 30000:
    #    break
    count += 1
    if count % 4 == 0:
        increment *= 4  # Doubling the increment

for algorithm in algorithms:
    for query in queries:
        for z in ['0', '1', '15']:
            input_file_path = data_path + algorithm + query + z + '.dat'
            output_file_path = data_path + algorithm + query + z + '.dat'  # New output file path for processed data

            output_data = ""
            with open(input_file_path, 'r') as file:
                for line in file:
                    stripped_line = line.strip()  # Remove any extra whitespace
                    value, data_point = stripped_line.split("\t")  # Split the line into value and data point
                    if value in values:
                        output_data += stripped_line + "\n"  # Append to the output with a newline

            # Write to the output file only if output_data is not empty
            if output_data.strip():  # Ensure output_data is not just whitespace
                with open(output_file_path, 'w') as output_file:
                    output_file.write(output_data.strip())
