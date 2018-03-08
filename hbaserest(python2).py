#!/usr/bin/env python
# coding=utf-8
import requests
import base64


class HBaseRest:
    baseUrl = ""

    def __init__(self, base_url):
        if str.endswith(base_url, "/"):
            self.baseUrl = base_url
        else:
            self.baseUrl = base_url + "/"

    def tables(self):
        """
        :return: a list of tables" name
        """
        tables = []
        ls = requests.get(self.baseUrl, headers={"Accept": "application/json"}).json()["table"]
        for t in ls:
            tables.append(t["name"])
        return tables

    def is_exist(self, table_name):
        """
        :param table_name:
        :return: boolean
        """
        return table_name in self.tables()

    def schema(self, table_name):
        """
        :param table_name:
        :return: a list of all columnFamilies" schema that in this table
        """
        return requests.get(self.baseUrl + table_name + "/schema", headers={"Accept": "application/json"}).json()[
            "ColumnSchema"]

    def region(self, table_name):
        """
        :param table_name:
        :return: a dict of this table"s region
        """
        ls = requests.get(self.baseUrl + table_name + "/regions", headers={"Accept": "application/json"}).json()[
            "Region"]
        if len(ls) > 0:
            return ls[0]
        else:
            return None

    @staticmethod
    def modify(base_url, table_name, _tuple, mode=0):
        """

        :param base_url:
        :param table_name:
        :param mode: 0: update (默认)
                     1: over_write
        :param _tuple: multi schema element in HColumnSchema object
        :return: boolean
        """
        column_schema_list = []
        for column in _tuple:
            column_schema_list.append(column.schema)
        body = {
            "ColumnSchema": column_schema_list
        }
        if mode == 0:
            return successful(requests.post(base_url + table_name + "/schema",
                                            headers={"Accept": "application/json", "Content-Type": "application/json"}
                                            , json=body))
        elif mode == 1:
            return successful(requests.put(base_url + table_name + "/schema",
                                           headers={"Accept": "application/json", "Content-Type": "application/json"}
                                           , json=body))
        else:
            return False

    def create(self, table_name, *h_column_schema_s):
        if self.is_exist(table_name):
            return False
        else:
            return self.modify(self.baseUrl, table_name, h_column_schema_s)

    def update(self, table_name, *h_column_schema_s):
        if not self.is_exist(table_name):
            return False
        else:
            return self.modify(self.baseUrl, table_name, h_column_schema_s)

    def over_write(self, table_name, *h_column_schema_s):
        return self.modify(self.baseUrl, table_name, h_column_schema_s, 1)

    def delete(self, table_name):
        """

        :param table_name:
        :return: boolean
        """
        if not self.is_exist(table_name):
            return False
        else:
            return successful(requests.delete(self.baseUrl + table_name + "/schema"))

    def add_cell(self, table_name, row_key, cell):
        """
        row_key是HBase中确实存在的,添加就是覆盖,否则,按row_key的字典顺序插入
        :param table_name:
        :param row_key:
        :param cell: HCell类型
        :return: boolean
        """
        # cells = [{
        #     "column": base64.encode(column_family + ":" + column),
        #     "$": base64.encode(value)
        # }]
        body = {"Row": [
            {
                "key": base64.b64encode(row_key),
                "Cell": [
                    cell.json
                ]
            }

        ]}
        return successful(requests.post(self.baseUrl + table_name + "/row",
                                        headers={"Accept": "application/json", "Content-Type": "application/json"}
                                        , json=body))

    def add_row(self, table_name, row):
        """

        :param table_name:
        :param row: HRow类型
        :return: boolean
        """
        body = {"Row": [
            row.json
        ]}
        return successful(requests.post(self.baseUrl + table_name + "/row",
                                        headers={"Accept": "application/json", "Content-Type": "application/json"}
                                        , json=body))

    def add_rows(self, table_name, h_rows):
        """
        一次插入多行
        :param table_name:
        :param h_rows: HRows类型
        :return:
        """
        body = {"Row": h_rows.json_list}
        return successful(requests.post(self.baseUrl + table_name + "/row",
                                        headers={"Accept": "application/json", "Content-Type": "application/json"}
                                        , json=body))

    def get_cell(self, table_name, row_key, column, label):
        """
        查找一个单元格数据,只有最后一个版本
        :param table_name:
        :param row_key:
        :param column:
        :param label:
        :return:
        """
        response = requests.get(self.baseUrl + table_name + "/" + row_key + "/" + column + ":" + label,
                                headers={"Accept": "application/json"})
        if successful(response):
            return response.json()
        else:
            return None

    def get_multi_version_cell(self, table_name, row_key, column, label, v):
        """
        查找一个单元格数据,有多个版本
        :param table_name:
        :param row_key:
        :param column:
        :param label:
        :param v: 获得一列中前v个版本的数据
        :return:
        """
        response = requests.get(self.baseUrl + table_name + "/" + row_key + "/" + column + ":" + label + "?v=" + str(v),
                                headers={"Accept": "application/json"})
        if successful(response):
            return response.json()
        else:
            return None

    def get_row(self, table_name, row_key):
        """
        查找一行数据,只有最后一个版本
        :param table_name:
        :param row_key:
        :return:
        """
        response = requests.get(self.baseUrl + table_name + "/" + row_key,
                                headers={"Accept": "application/json"})
        if successful(response):
            return response.json()
        else:
            return None

    def get_multi_version_row(self, table_name, row_key, v):
        """
        按行键查找一行数据,有多个版本
        :param table_name:
        :param row_key:
        :param v:
        :return:
        """
        response = requests.get(self.baseUrl + table_name + "/" + row_key + "?v=" + str(v),
                                headers={"Accept": "application/json"})
        if successful(response):
            return response.json()
        else:
            return None

    @staticmethod
    def get_rows_by_scanner(base_url, table_name, scanner):
        response = requests.put(base_url + table_name + "/scanner",
                                headers={"Accept": "application/json", "Content-Type": "text/xml"}
                                , data=scanner)
        if successful(response):
            url = response.headers.get("Location")
            response = requests.get(url, headers={"Accept": "application/json"})
            requests.delete(url)
            if successful(response):
                return response.json()
            else:
                return None

    def get_rows_by_rang_filter(self, table_name, start_row_key, end_row_key, max_number):
        """
        通过行键的范围查找数据
        :param table_name:
        :param start_row_key:开始行键
        :param end_row_key:结束行键
        :param max_number: 设定最大单元数
        :return:
        """
        scanner = '<Scanner startRow="{}" endRow="{}" batch="{}"></Scanner>'. \
            format(base64.b64encode(start_row_key), base64.b64encode(end_row_key), max_number)
        return self.get_rows_by_scanner(self.baseUrl, table_name, scanner)

    def get_rows_by_prefix_filter(self, table_name, prefix_filter, max_number):
        """
        通过行键的前缀查找数据
        :param table_name:
        :param prefix_filter: 前缀过滤器
        :param max_number: 设定最大单元数
        :return:
        """
        scanner = '<Scanner batch="{}"><filter>{}</filter></Scanner>'. \
            format(max_number, '{' + '"type":"PrefixFilter","value":"{}"'.format(prefix_filter) + '}')
        return self.get_rows_by_scanner(self.baseUrl, table_name, scanner)


# Checks the request object to see if the call was successful
def successful(request):
    if 200 <= request.status_code <= 299:
        return True
    else:
        return False


def standard(json_object):
    """
    将含有base64编码的json数据解码输出
    :param json_object:{u'Row': [{u'Cell': [{u'column': u'ZDpjb2x1bW4x', u'timestamp': 1518609441465, u'$': u'MDE='}], u'key': u'cm93MA=='}]}
    :return: json_object:{u'Row': [{u'Cell': [{u'column': 'd:column1', u'timestamp': 1518609441465, u'$': '01'}], u'key': 'row0'}]}
    """
    if json_object is not None:
        row_list = json_object['Row']
        for row in row_list:
            row['key'] = base64.b64decode(row['key'])
            cell_list = row['Cell']
            for cell in cell_list:
                cell['column'] = base64.b64decode(cell['column'])
                cell['$'] = base64.b64decode(cell['$'])
        return json_object
    else:
        return None


class HCell:
    def __init__(self, family_name, label_name, value):
        self.json = {
            "column": base64.b64encode(family_name + ":" + label_name),
            "$": base64.b64encode(str(value))
        }


class HRow:
    def __init__(self, row_key, *cells):
        json_list = []
        for cell in cells:
            json_list.append(cell.json)
        self.json = {
            "key": base64.b64encode(row_key),
            "Cell": json_list
        }


class HRows:
    def __init__(self, *row_s):
        self.json_list = []
        for row in row_s:
            self.json_list.append(row.json)

    def put(self, row):
        self.json_list.append(row.json)


class HColumnSchema:

    def __init__(self, name):
        self.schema = {"name": name}

    def set_keep_deleted_cells(self, index):
        """

        :param index: "FALSE":Deleted Cells are not retained.(默认)
                       "TRUE":Deleted Cells are retained until they are removed by other means such TTL or VERSIONS.
                       "TTL":Deleted Cells are retained until the delete marker expires due to TTL.
        :return:
        """
        self.schema["KEEP_DELETED_CELLS"] = ["FALSE", "TRUE", "TTL"][index]

    def set_version(self, version):
        """

        :param version: number
        :return:
        """
        self.schema["VERSIONS"] = version

    def set_max_save_time(self, time):
        """

        :param time: number
        :return:
        """
        self.schema["TTL"] = time

    def set_block_size(self, size):
        """

        :param size    column_schema1 = HColumnSchema("family1")
    column_schema1.set_version(3)
    column_schema1.set_keep_deleted_cells(1)
    column_schema2 = HColumnSchema("family2")
    column_schema2.set_version(3)
    column_schema2.set_keep_deleted_cells(1)
    print(rest.create("test1", column_schema1.schema, column_schema2.schema)): number
        :return:
        """
        self.schema["BLOCKSIZE"] = size

    def set_block_cache(self, index):
        """

        :param index: "true":  use cache (默认)
                      "false": not use cache
        :return:
        """
        self.schema["BLOCKCACHE"] = ["true", "false"][index]

    def set_data_block_encode(self, index):
        """

        :param index: "NONE":(默认)
                      "Prefix":
                      "Diff":
                      "Fast_Diff":
                      "Prefix_Tree":
        :return:
        """
        self.schema["DATA_BLOCK_ENCODING"] = ["NONE", "Prefix", "Diff", "Fast_Diff", "Prefix_Tree"][index]

    def set_bloom_filter(self, index):
        """

        :param index: "ROW" : (默认)
                      "ROWCOL":
        :return:
        """
        self.schema["BLOOMFILTER"] = ["ROW", "ROWCOL"][index]

    def set_compression(self, index):
        """

        :param index: "NONE": (默认)
                      "GZIP" :
                      "LZO" :
                      "Snappy":
        :return:
        """
        self.schema["COMPRESSION"] = ["NONE", "GZIP", "LZO", "Snappy"][index]

    def set_in_memory(self, index):
        """

        :param index: "true" or "false"
        :return:
        """
        self.schema["IN_MEMORY"] = ["true", "false"][index]


# test
if __name__ == '__main__':
    rest = HBaseRest("http://localhost:6077")
    # 列出所有表名
    print(rest.tables())
    # 创建表
    print(rest.create("test", HColumnSchema("family1"), HColumnSchema("family2")))
    # 判断表是否存在
    print(rest.is_exist("test"))
    # 查看表的域
    print(rest.region("test"))
    # 查看表的结构
    print(rest.schema("test"))
    # 更新表的结构,update是更新,over_write是覆盖
    column_schema = HColumnSchema("family3")
    column_schema.set_version(3)
    print(rest.update("test", column_schema))
    print(rest.schema("test"))
    # 插入一个单元格数据
    print(rest.add_cell("test", "row1",
                        HCell("family1", "label1", "111")))
    # 插入一整行数据
    print(rest.add_row("test", HRow("row2",
                                    HCell("family1", "label1", 211),
                                    HCell("family1", "label2", 212),
                                    HCell("family2", "label1", 221),
                                    HCell("family2", "label2", 222),
                                    HCell("family3", "label1", 231),
                                    HCell("family3", "label2", 232))))
    # 插入多行数据
    rows = HRows()
    rows.put(HRow("row3",
                  HCell("family1", "label1", 311),
                  HCell("family1", "label2", 312),
                  HCell("family2", "label1", 321),
                  HCell("family2", "label2", 322),
                  HCell("family3", "label1", 331),
                  HCell("family3", "label2", 332)))
    rows.put(HRow("row4",
                  HCell("family1", "label1", 411),
                  HCell("family1", "label2", 412),
                  HCell("family2", "label1", 421),
                  HCell("family2", "label2", 422),
                  HCell("family3", "label1", 431),
                  HCell("family3", "label2", 432)))
    print(rest.add_rows("test", rows))
    # 查找一个单元格数据,只有最后一个版本
    print(standard(rest.get_cell("test", "row1", "family1", "label1")))
    # 查找一个单元格数据,有多个版本
    print(standard(rest.get_multi_version_cell("test", "row1", "family3", "label1", 3)))
    # 查找一行数据,只有最后一个版本
    print(standard(rest.get_row("test", "row2")))
    # 查找一行数据,有多个版本
    print(standard(rest.get_multi_version_row("test", "row2", 3)))
    # 按行键的前缀过滤,查找多行数据,指定最多只查找10个单元格数据
    print(standard(rest.get_rows_by_prefix_filter("test", "row", 10)))
    # 按行键的范围过滤,查找多行数据,指定最多只查找10个单元格数据
    print(standard(rest.get_rows_by_prefix_filter("test", "row", 10)))
    # 删除表
    print(rest.delete("test"))
