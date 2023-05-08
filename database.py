import pymysql


class DB(object):
    def __init__(self,config):
        try:
            self.con = pymysql.connect(
                host=config['host'],
                port=config['port'],
                user=config['user'],
                passwd=config['password'],
                db=config['db_name'],  # 数据库名
                charset=config['charset']
            )
            print('connected!')
        except pymysql.Error as e:
            print("Error %d：%s" % (e.args[0], e.args[1]))
            exit()
        self.cursor = self.con.cursor()  # 创建游标对象

    # 增加信息
    def add_data(self, sql, data=[]):
        try:
            self.cursor.execute(sql, data)
            self.con.commit()
            print('new request added!')
        except Exception as e:
            self.con.rollback()
            print("Error", e)

    # 修改或删除信息
    def update_or_delete(self, sql):
        try:
            self.cursor.execute(sql)
            self.con.commit()
        except Exception as e:
            self.con.rollback()
            print("Error ", e.args[0])

    # 查询一条信息
    def search_one(self, sql):
        try:
            self.cursor.execute(sql)
            res = self.cursor.fetchone()
        except Exception as e:
            return "Error " + e.args[0]
        return res

    # 查询全部信息
    def search_all(self, sql):
        try:
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
        except Exception as e:
            return "Error " + e.args[0]
        for r in res:
            yield r

    # 关闭游标和数据库的连接
    def __del__(self):
        self.cursor.close()
        self.con.close()


def main():
    db = DB()  # 实例化一个对象
    # 插入一条数据
    sql = 'insert into students (sno, s_name, gender, age, height, speciality) values(%s, %s, %s, %s, %s, %s)'
    data = ('1010', '小小', '女', '18', '1.70', '会计学')
    db.add_data(sql, data)

    # 查询一条信息(上面所增加的信息)
    sql = "select * from students where s_name='小小';"
    res = db.search_one(sql)
    print(res)
    # 修改信息
    sql = "update students set s_name='大大' where s_name='小小';"
    db.update_or_delete(sql)
    # 查询一条信息(上面所增加的信息)
    sql = "select * from students where s_name='大大';"
    res = db.search_one(sql)
    print(res)
    # 删除信息
    sql = "delete from students where s_name='大大';"
    db.update_or_delete(sql)
    # 查询所有信息
    sql = 'select * from students;'
    for data in db.search_all(sql):
        print(data)


if __name__ == '__main__':
    main()

