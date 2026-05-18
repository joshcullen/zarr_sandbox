
### Test for reading .txt file from public NODD bucket ###


# 1. Define the file path
filepath = 'https://storage.googleapis.com/nmfs_odp_swfsc/ESD/climate-ecosystems/test.txt'

# 2. Read it directly using standard R functions
# Use read.table, read.csv, or readLines depending on the text structure
text_data <- readLines(filepath)

print(text_data)
