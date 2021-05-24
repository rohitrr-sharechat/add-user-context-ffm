def readSqlFile(file_path, lang, rating_def = "", q0_table = "", 
                q1_table = "", q2_table = "", train_q1_table = "",
                end_time = "", days = 2,
               common_posts_end_time = "", common_posts_days = 2):
    with open (file_path, "r") as file:
        sql_command=file.read()
        sql_command = sql_command.format(
            common_posts_end_time = common_posts_end_time if common_posts_end_time == "" else \
                                    str(common_posts_end_time.strftime('%Y-%m-%d %H:%M:%S')),
            common_posts_days = common_posts_days,
            days=days,
            end_time=end_time if end_time == "" else \
                        str(end_time.strftime('%Y-%m-%d %H:%M:%S')),
            language=lang,
            rating_def=rating_def,
            q0table=q0_table,
            q1table=q1_table,
            q2table=q2_table,
            train_q1table = train_q1_table
        )
    return sql_command