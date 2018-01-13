import logging
CREATE_TABLE_KEY_WORD = "CREATE TABLE"
AS_KEY_WORD = """AS"""
SELECT_KEY_WORD="SELECT"

def get_table_name_and_selected_phase_from_create_as_sql(created_table_sql):
    tokens = created_table_sql.split(AS_KEY_WORD)
    create_table_phrase = tokens[0]
    try :
        selected_phase = created_table_sql[created_table_sql.index(SELECT_KEY_WORD):]
    except Exception as  e :
        error_message = "Exception when parsing CREATE TABLE command...%s"%(e.message)
        logging.info()
        raise  Exception(error_message=error_message)
    #print("create table phrase :%s"%(create_table_phrase))
    table_name = create_table_phrase.split(CREATE_TABLE_KEY_WORD)[1].strip()
    return table_name,selected_phase



# table_name,selected_phase= get_table_name_and_selected_phase_from_create_as_sql(created_table_sql=
#                                            """
#                                            CREATE TABLE ABC
#                                            AS
#                                            SELECT 1 AS COLUMN1
#                                            """
#                                            )
# print("table_name =%s"%(table_name))
# print("selected_phase =%s"%(selected_phase))


