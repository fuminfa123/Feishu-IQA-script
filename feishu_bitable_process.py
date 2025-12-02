'''飞书多维表格数据处理脚本（适配GitHub Actions）'''
import os
import json
from datetime import datetime
import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *
from lark_oapi.api.drive.v1 import *
import requests
import pyexcel
import pandas as pd
import io

# ====================== 配置读取（从环境变量） ======================
APP_ID = os.getenv("FEISHU_APP_ID")
APP_SECRET = os.getenv("FEISHU_APP_SECRET")
DWBG_TOKEN = os.getenv("DWBG_TOKEN")
DWBG_TABLE_ID = os.getenv("DWBG_TABLE_ID")
ROW_ID = os.getenv("ROW_ID")

# 校验必要参数
def validate_env():
    missing = []
    if not APP_ID:
        missing.append("FEISHU_APP_ID")
    if not APP_SECRET:
        missing.append("FEISHU_APP_SECRET")
    if not DWBG_TOKEN:
        missing.append("DWBG_TOKEN")
    if not DWBG_TABLE_ID:
        missing.append("DWBG_TABLE_ID")
    if not ROW_ID:
        missing.append("ROW_ID")
    if missing:
        raise Exception(f"缺少必要环境变量：{', '.join(missing)}")

'''飞书多维表格核心函数'''
def 获取访问令牌(APP_ID,APP_SECRET):
    """获取访问令牌"""
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/"
    headers = {"Content-Type": "application/json"}
    data = {
        "app_id": APP_ID,
        "app_secret": APP_SECRET
    }
    try:
        response = requests.post(url, headers=headers, json=data, timeout=10)
        response.raise_for_status()
        response_data = response.json()

        if response_data.get("code") == 0:
            return response_data.get("tenant_access_token")
        else:
            raise Exception(f"获取access_token失败: 错误码={response_data.get('code')}, 消息={response_data.get('msg')}")
    except requests.exceptions.RequestException as e:
        raise Exception(f"获取access_token网络请求失败: {str(e)}")

def 获取多维表格中附件的链接(访问令牌,DWBG_TOKEN,DWBG_TABLE_ID,行ID=None,附件字段名="上传附件"):
    """提取多维表格指定行的附件原始URL"""
    if not 行ID:
        raise ValueError("❌ 行ID不能为空，请传入目标行的record_id")

    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{DWBG_TOKEN}/tables/{DWBG_TABLE_ID}/records/search"
    headers = {
        "Authorization": f"Bearer {访问令牌}",
        "Content-Type": "application/json"
    }
    request_data = {"page_size": 100, "page_token": ""}
    all_attachments = []

    while True:
        try:
            resp = requests.post(url, headers=headers, json=request_data, timeout=15)
            resp.raise_for_status()
            result = resp.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f"❌ 请求接口失败: {str(e)}")

        if result["code"] != 0:
            raise Exception(f"❌ 读取表格失败: {result['msg']} (code: {result['code']})")

        # 匹配目标行ID
        target_record = None
        for record in result["data"]["items"]:
            if record["record_id"] == str(行ID):
                target_record = record
                break

        if target_record:
            fields = target_record.get("fields", {})
            attachments = fields.get(附件字段名, [])
            if not attachments:
                raise Exception(f"❌ 行ID [{行ID}] 的「{附件字段名}」列无附件")

            # 筛选Excel附件
            for att in attachments:
                att_url = att.get("url")
                att_name = att.get("name", "")
                if att_url and att_name.endswith((".xlsx", ".xls")):
                    print(f"✅ 行ID [{行ID}] 提取到附件: {att_name} | URL: {att_url[:50]}...")
                    all_attachments.append((att_url, att_name))
            break

        if not result["data"].get("has_more"):
            break
        request_data["page_token"] = result["data"]["page_token"]

    if not all_attachments:
        raise Exception(f"❌ 行ID [{行ID}] 的「{附件字段名}」列未找到Excel附件")

    return all_attachments

def 在线解析表格为二维数据(访问令牌, 文件临时链接, 文件名称):
    """在线解析Excel为二维列表字典"""
    headers = {
        "Authorization": f"Bearer {访问令牌}",
        "User-Agent": "Mozilla/5.0 (Linux; x86_64) AppleWebKit/537.36"
    }
    try:
        resp = requests.get(文件临时链接, headers=headers, timeout=300, stream=True)
        resp.raise_for_status()
        excel_content = io.BytesIO()
        for chunk in resp.iter_content(chunk_size=1024*1024):
            if chunk:
                excel_content.write(chunk)
        excel_content.seek(0)
        print(f"✅ 成功下载在线附件: {文件名称}")
    except Exception as e:
        print(f"❌ 下载在线附件失败: {str(e)}")
        return None

    # 优先用pyexcel解析
    try:
        工作表字典 = {}
        book = pyexcel.get_book(
            file_type=文件名称.split('.')[-1],
            file_content=excel_content.getvalue()
        )

        for sheet_name in book.sheet_names():
            二维列表 = book[sheet_name].rows()
            二维列表 = [[cell if cell is not None else "" for cell in row] for row in 二维列表]
            工作表字典[sheet_name] = 二维列表

        print(f"✅ pyexcel解析完成，共{len(工作表字典)}个Sheet")
        return 工作表字典
    except Exception as e:
        print(f"⚠️ pyexcel解析失败，降级用pandas: {str(e)}")
        excel_content.seek(0)

    # pandas降级解析
    try:
        工作表字典 = {}
        excel_file = pd.ExcelFile(excel_content)

        for sheet_name in excel_file.sheet_names:
            engine = "xlrd" if 文件名称.lower().endswith('.xls') else "openpyxl"
            df = pd.read_excel(excel_file, sheet_name=sheet_name, engine=engine)

            header_row = df.columns.tolist()
            data_rows = df.values.tolist()
            二维列表 = [header_row] + data_rows

            # 处理空值和特殊类型
            二维列表 = [
                [
                    "" if pd.isna(cell)
                    else str(cell) if not isinstance(cell, (str, int, float, bool))
                    else cell
                    for cell in row
                ]
                for row in 二维列表
            ]
            工作表字典[sheet_name] = 二维列表

        print(f"✅ pandas解析完成，共{len(工作表字典)}个Sheet")
        return 工作表字典
    except Exception as e:
        print(f"❌ pandas解析失败: {str(e)}")
        return None

def 日期单元格转变(批次):
    """日期格式转换"""
    if isinstance(批次, datetime):
        return 批次.strftime("%Y-%m-%d")
    elif isinstance(批次, str):
        try:
            return 批次[:4] + "-" + 批次[5:7] + "-" + 批次[8:10]
        except:
            return "格式错误"
    else:
        try:
            return datetime.fromtimestamp(批次).strftime("%Y%m%d")
        except:
            return "不支持的格式"

def 判断品项(内容):
    """品项匹配"""
    内容_str = str(内容)
    if "速冻调理九块鸡" in 内容_str:
        return "速冻调理九块鸡2.0"
    elif "冷冻调味鸡架" in 内容_str:
        return "冷冻调味鸡架"
    elif "速冻调理烤翅用鸡翅中尖" in 内容_str:
        return "速冻调理烤翅用鸡翅中尖2.0"
    elif "速冻调理烤翅用鸡翅根2.0" in 内容_str:
        return "速冻调理烤翅用鸡翅根2.0"
    elif "速冻调理辣翅用鸡翅根2.0" in 内容_str:
        return "速冻调理辣翅用鸡翅根2.0"
    elif "速冻调理鸡翅边肉" in 内容_str:
        return "速冻调理鸡翅边肉2.0"
    elif "速冻调理115汉堡用鸡腿肉2.0" in 内容_str:
        return "速冻调理115汉堡用鸡腿肉2.0"
    elif "速冻调理90汉堡用鸡腿肉（香辣）2.0" in 内容_str:
        return "速冻调理90汉堡用鸡腿肉（香辣）2.0"
    elif "速冻调理90汉堡用鸡腿肉（ETC）" in 内容_str:
        return "速冻调理90汉堡用鸡腿肉（ETC）2.0"
    elif "冷冻烧烤风味带皮鸡腿肉" in 内容_str:
        return "冷冻烧烤风味带皮鸡腿肉"
    elif "冷冻原味鸡风味带皮鸡腿肉" in 内容_str:
        return "冷冻原味鸡风味带皮鸡腿肉"
    elif "速冻调理烤翅用翅中尖" in 内容_str:
        return "速冻调理烤翅用鸡翅中尖"
    elif "速冻调理鸡腿肉丁" in 内容_str:
        return "速冻调理鸡腿肉丁2.0"
    elif "速冻调理鸡腿肉条" in 内容_str:
        return "速冻调理鸡腿肉条2.0"
    elif "速冻调理辣翅用鸡翅中2.0" in 内容_str:
        return "速冻调理辣翅用鸡翅中2.0"
    elif "冷冻腌制香辣风味鸡腿肉" in 内容_str:
        return "冷冻腌制香辣风味鸡腿肉"
    elif "火塘烧烤风味翅中尖" in 内容_str:
        return "火塘烧烤风味翅中尖"
    elif "九块鸡" in 内容_str:
        return "九块鸡"
    elif "115汉堡腿肉" in 内容_str:
        return "115汉堡腿肉"
    elif "90汉堡腿肉" in 内容_str:
        return "90汉堡腿肉"
    elif "腿肉条" in 内容_str:
        return "腿肉条"
    elif "腿肉丁" in 内容_str:
        return "腿肉丁"
    elif "翅边肉" in 内容_str:
        return "鸡翅边肉"
    elif "烤翅用翅中尖" in 内容_str:
        return "烤翅用翅中尖"
    elif "烤翅用翅根" in 内容_str:
        return "烤翅用翅根"
    elif "辣翅用翅根" in 内容_str:
        return "辣翅用翅根"
    elif "辣翅用翅中" in 内容_str:
        return "辣翅用翅中"
    elif "鸡翅尖" in 内容_str:
        return "鸡翅尖"
    elif "琵琶腿" in 内容_str and "140" in 内容_str:
        return "琵琶腿（140）"
    elif "琵琶腿" in 内容_str and "110" in 内容_str:
        return "琵琶腿（110）"
    elif "大翅根" in 内容_str:
        return "大翅根（60-70）"
    else:
        return None

def 更新飞书表格(应用ID, 应用密匙, DWBG_TOKEN, DWBG_TABLE_ID, 行ID, 上传数据结构):
    """更新飞书多维表格行数据"""
    client = lark.Client.builder() \
        .app_id(应用ID) \
        .app_secret(应用密匙) \
        .log_level(lark.LogLevel.DEBUG) \
        .build()

    request: UpdateAppTableRecordRequest = UpdateAppTableRecordRequest.builder() \
        .app_token(DWBG_TOKEN) \
        .table_id(DWBG_TABLE_ID) \
        .record_id(行ID) \
        .request_body(AppTableRecord.builder()
                      .fields(上传数据结构)
                      .build()) \
        .build()

    response: UpdateAppTableRecordResponse = client.bitable.v1.app_table_record.update(request)

    if not response.success():
        error_msg = f"更新表格失败 - code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}"
        if response.raw and response.raw.content:
            try:
                resp_content = json.loads(response.raw.content)
                error_msg += f", 详细响应: {json.dumps(resp_content, indent=2, ensure_ascii=False)}"
            except:
                error_msg += f", 响应内容: {response.raw.content}"
        raise Exception(error_msg)
    print(f"✅ 行ID [{行ID}] 表格更新成功")

# ====================== 核心业务逻辑 ======================
def main():
    # 1. 校验环境变量
    validate_env()
    print("✅ 环境变量校验通过")

    # 2. 获取访问令牌
    访问令牌 = 获取访问令牌(APP_ID, APP_SECRET)
    print(f"✅ 获取访问令牌成功: {访问令牌[:20]}...")

    # 3. 获取附件链接
    try:
        附件列表 = 获取多维表格中附件的链接(访问令牌, DWBG_TOKEN, DWBG_TABLE_ID, ROW_ID, "上传附件")
    except Exception as e:
        raise Exception(f"获取附件链接失败: {str(e)}")

    # 4. 解析Excel并处理数据
    数据字典 = {}
    for 附件元组 in 附件列表:
        文件临时链接, 文件名称 = 附件元组
        print(f"\n开始解析附件: {文件名称}")

        # 解析Excel
        工作表字典 = 在线解析表格为二维数据(访问令牌, 文件临时链接, 文件名称)
        if not 工作表字典:
            raise Exception(f"附件 {文件名称} 解析失败")

        # 处理"监测数据"工作表
        if "监测数据" in 工作表字典:
            监测数据 = 工作表字典["监测数据"]
            取值字典 = 数据字典.setdefault("监测数据", {})

            for 行数, 一行内容 in enumerate(监测数据):
                if 行数 > 1 and len(一行内容) >= 16 and 一行内容[13]:
                    # 提取字段
                    工厂全称 = 一行内容[1]
                    工厂名称 = 新检查工厂字典.get(工厂全称)
                    产品品项 = 判断品项(一行内容[3])

                    if not 产品品项 or not 工厂名称:
                        continue  # 跳过无法匹配的品项/工厂

                    工艺品类 = 一行内容[2]
                    模块 = 一行内容[4]
                    工序 = 一行内容[5]
                    控制组 = 一行内容[6]
                    控制点 = 一行内容[7]
                    检测时间 = 日期单元格转变(一行内容[9])
                    状态 = 一行内容[13]
                    检测值 = 一行内容[14]
                    控制标准 = 一行内容[15]

                    # 筛选生产过程监测-原料鸡肉-产品品质检查
                    if 模块 == "生产过程监测" and 工艺品类 == "原料鸡肉" and 工序 == "产品品质检查":
                        # 构建嵌套字典
                        取值列表 = 取值字典.setdefault(工厂名称, {}) \
                                            .setdefault(检测时间[:10], {}) \
                                            .setdefault(产品品项, {}) \
                                            .setdefault(状态, {}) \
                                            .setdefault(str(控制组), {}) \
                                            .setdefault(str(控制点), [])
                        取值列表.append(检测值)

    # 5. 统计数据并更新表格
    上传数据结构2 = {}
    检查工厂字典 = {
        '光泽二厂': ['福建圣农发展股份有限公司中坊第二肉鸡加工厂'],
        '光泽三厂': ['福建圣农发展股份有限公司中坊第三肉鸡加工厂'],
        '光泽四厂': ['福建圣农发展股份有限公司中坊第四肉鸡加工厂'],
        '光泽六厂': ['福建圣农发展股份有限公司肉鸡加工六厂'],
        '浦城一厂': [
            '福建圣农发展（浦城）有限公司一厂',
            '福建圣农发展（浦城）有限公司肉鸡加工一厂'
        ],
        '浦城二厂': [
            '福建圣农发展（浦城）有限公司肉鸡加工二厂',
            '福建圣农发展（浦城）有限公司二厂'
        ],
        '政和工厂': ['圣农发展（政和）有限公司'],
        '圣越工厂': ['甘肃圣越农牧发展有限公司']
    }
    新检查工厂字典 = {值: 键 for 键, 值列表 in 检查工厂字典.items() for 值 in 值列表}

    for 项, 工厂字典 in 数据字典.items():
        for 工厂名称, 时间字典 in 工厂字典.items():
            偏差统计列表 = []
            翅类中值统计列表 = []

            for 监测时间, 品项字典 in 时间字典.items():
                产品信息 = "、".join(品项字典.keys())
                偏差明细列表 = []

                for 产品品项, 状态字典 in 品项字典.items():
                    for 状态, 控制组字典 in 状态字典.items():
                        # 统计不合格偏差
                        if 状态 == "不合格":
                            for 控制组, 控制点字典 in 控制组字典.items():
                                for 控制点, 录入值 in 控制点字典.items():
                                    偏差明细列表.append(f"{产品品项}:{控制点}")

                        # 统计翅类单枚重量
                        if "翅中" in 产品品项 or "翅根" in 产品品项:
                            for 控制组, 控制点字典 in 控制组字典.items():
                                for 控制点, 录入值 in 控制点字典.items():
                                    if "单枚重量" in 控制点:
                                        监测值 = "、".join(录入值).replace(",", "、")
                                        翅类中值统计列表.append(f"{监测时间}*{工厂名称}*{产品品项}*{监测值}")

                # 构建偏差信息
                if 偏差明细列表:
                    偏差信息 = "、".join(偏差明细列表)
                    偏差统计列表.append(f"{监测时间}*{工厂名称}*{产品信息}*{偏差信息}")
                else:
                    偏差统计列表.append(f"{监测时间}*{工厂名称}*{产品信息}")

            # 组装上传数据
            if 偏差统计列表:
                偏差合并信息 = ",".join(偏差统计列表)
                上传数据结构2[f"{工厂名称}（偏差）"] = 偏差合并信息

            if 翅类中值统计列表:
                翅类中值合并信息 = ",".join(翅类中值统计列表)
                上传数据结构2[f"{工厂名称}（翅类中值）"] = 翅类中值合并信息

    # 6. 更新飞书表格
    if 上传数据结构2:
        print(f"✅ 准备更新表格，数据结构: {json.dumps(上传数据结构2, ensure_ascii=False)}")
        更新飞书表格(APP_ID, APP_SECRET, DWBG_TOKEN, DWBG_TABLE_ID, ROW_ID, 上传数据结构2)
    else:
        print("ℹ️ 无需要更新的数据")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ 脚本执行失败: {str(e)}")
        exit(1)  # 退出码非0，标记GitHub Actions任务失败
