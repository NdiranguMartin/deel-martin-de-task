import tasks 

# extract the data from the api and load it to the warehouse
tasks.extract_load()

# run a transformation to create facts and dimensions
tasks.transform()

# answer some business questions
tasks.analytics()