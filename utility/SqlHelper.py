from pymysql import connect
import logging


class MySqlHelper(object):
    """SqlHelper for MySQL"""

    def __init__(self, **kwargs):
        self.__kwargs = kwargs

    def __enter__(self):
        self.__connection = connect(**self.__kwargs)
        # self.__connection = connect(host=self.__kwargs.get('host'),
        #                             port=self.__kwargs.get('port'),
        #                             user=self.__kwargs.get('user'),
        #                             password=self.__kwargs.get('password'),
        #                             database=self.__kwargs.get('database'))
        self.__cursor = self.__connection.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.__connection is not None:
            self.__connection.close()
        if self.__cursor is not None:
            self.__cursor.close()

    def execute_datatable(self, sql, *args):
        """fetch all data records by once"""
        self.__cursor.execute(sql, *args)
        return self.__cursor.fetchall()

    def execute_datareader(self, sql, *args):
        """generator of the query result"""

        self.__cursor.execute(sql, *args)
        while True:
            row = self.__cursor.fetchone()
            if not row:
                break
            yield row

    def execute_nonquery(self, sql, *args):
        """
        execute nonquery including insert/update/delete.it supports batch records by transaction
        :param sql:
        :param args:
        :return:
        """

        try:
            if len(args) > 1:
                print(123)
                affected = self.__cursor.executemany(sql, *args)
            else:
                affected = self.__cursor.execute(sql, *args)

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
        students = helper.execute_datatable("select * from students where id<=%s", [4])
        print(students)
        #
        # students = helper.execute_datareader("select * from students where id<=%s", [4])
        # for s in students:
        #     print(s)

        # affected = helper.execute_nonquery("delete from students where id between %s and %s", [4, 8])
        # print("Affected rows: %d" % affected)

        # records = [("Tom", 1, 2), ("Jerry", 2, 1)]
        # affected = helper.execute_nonquery("insert into students (`name`,gender,classId) values (%s,%s,%s)", records)
        # print("Affected rows: %d" % affected)
