from pymysql import connect
import logging


class MySqlHelper(object):
    """SqlHelper for MySQL"""

    def __init__(self, host, user, password, database, port=3306, charset="utf8"):
        self.__host = host
        self.__port = port
        self.__user = user
        self.__password = password
        self.__database = database
        self.__charset = charset

    def __execute(self, fun):
        with connect(host=self.__host, port=self.__port, user=self.__user, password=self.__password,
                     database=self.__database,
                     charset=self.__charset).cursor() as cur:
            return fun(cur)

    def execute_datatable(self, sql, *args):
        """fetch all data records by once"""

        def execute(cur):
            cur.execute(sql, *args)
            return cur.fetchall()

        return self.__execute(execute)

    def execute_datareader(self, sql, *args):
        """generator of the query result"""

        def execute(cur):
            cur.execute(sql, *args)
            while True:
                row = cur.fetchone()
                if not row:
                    break
                yield row

        return self.__execute(execute)

    def execute_nonquery(self, sql, *args):
        """
        execute nonquery including insert/update/delete.it supports batch records by transaction
        :param sql:
        :param args:
        :return:
        """

        def execute(cur):
            try:
                if len(*args) > 1:
                    affected = cur.executemany(sql, *args)
                else:
                    affected = cur.execute(sql, *args)

                cur.connection.commit()
                return affected
            except Exception as ex:
                cur.connection.rollback()
                logging.error(ex, exc_info=1, stack_info=1)
                return 0

        return self.__execute(execute)


if __name__ == '__main__':
    db_config = {
        'host': '127.0.0.1',
        'port': 3306,
        'user': 'root',
        'password': '123456',
        'database': 'test'
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
