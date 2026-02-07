#short python script to generate SQL create table statement for 
#retrosheet gamelog table using one column list of fields.

gamelog_fields_file="../data/retrosheet/raw_gamelogs/retrosheet_gamelog_fields.txt"

cols = []

with open(gamelog_fields_file) as f:
    for line in f:
        col = line.strip() #remove white space
        col = col.lower() #make sure field is all lower case
        cols.append(f"{col} TEXT") #default to TEXT data type for all columns for ease of ingestion

sql = "CREATE TABLE retrosheet_gamelogs (\n"
sql += ",\n".join(cols)
sql += "\n);"

#create table statement is printed to stdout - direct it to sql file
print(sql)
