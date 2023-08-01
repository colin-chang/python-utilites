from pymysql import connect
import logging


class MySqlHelper(object):
    """SqlHelper for MySQL"""

    def __init__(self, **kwargs):
        self.connection = connect(host=kwargs.get('host'),
                                  port=kwargs.get('port'),
                                  user=kwargs.get('user'),
                                  password=kwargs.get('password'),
                                  database=kwargs.get('database'),
                                  charset=kwargs.get('charset'))

    def __del__(self):
        self.connection.close()

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
                if len(args) > 1:
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
