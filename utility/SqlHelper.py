from pymysql import connect
import logging


class MySqlHelper(object):
    def __init__(self, **kwargs):
        self.__connection = connect(**kwargs)
        self.__cursor = self.__connection.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.__connection is not None:
            self.__connection.close()
        if self.__cursor is not None:
            self.__cursor.close()

    def execute_datatable(self, sql, *args):
        self.__cursor.execute(sql, args)
        return self.__cursor.fetchall()

    def execute_datareader(self, sql, *args):

        self.__cursor.execute(sql, args)
        while True:
            row = self.__cursor.fetchone()
            if not row:
                break
            yield row

    def execute_nonquery(self, sql, *args, many=False):
        try:
            if many:
                affected = self.__cursor.executemany(sql, [("Tom", 1, 2), ("Jerry", 2, 1)])
            else:
                affected = self.__cursor.execute(sql, args)
            self.__cursor.connection.commit()
            return affected
        except Exception as ex:
            self.__cursor.connection.rollback()
            logging.error(ex, exc_info=1, stack_info=1)
            return 0


if __name__ == '__main__':
    db_config = {
        'host': '192.168.192.5',
        'port': 3306,
        'user': 'root',
        'password': 'xiaoyang@123123@db',
        'database': 'hc-dev-gam'
    }
    with MySqlHelper(**db_config) as helper:
        students = helper.execute_datatable("select * from students where id<=%s", 4)
        print(students)

        # students = helper.execute_datareader("select * from students where id between %s and %s", 4, 8)
        # for s in students:
        #     print(s)

        # affected = helper.execute_nonquery("delete from students where id between %s and %s", 10, 20)
        # print("Affected rows: %d" % affected)

        # records = (("Tom", 1, 2), ("Jerry", 2, 1))
        # affected = helper.execute_nonquery("insert into students (`name`,gender,classId) values (%s,%s,%s)", *records,
        #                                    many=True)
        # print("Affected rows: %d" % affected)
