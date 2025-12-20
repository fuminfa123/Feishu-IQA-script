'''飞书多维表格需要的库'''
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

'''飞书多维表格函数'''
def 获取访问令牌(APP_ID, APP_SECRET):
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

def 飞书上传素材(文件路径, DWBG_TOKEN, 应用ID, 应用密匙):
    """
    使用飞书官方SDK上传文件到多维表格
    :param 文件路径: 本地文件路径
    :param DWBG_TOKEN: 多维表格的app_token
    :param 访问令牌: 用户访问令牌
    :return: 文件上传成功返回file_token，失败返回None
    """
    # 验证文件是否存在
    if not os.path.exists(文件路径):
        print(f"错误：文件不存在 - {文件路径}")
        return None
    if not os.path.isfile(文件路径):
        print(f"错误：不是有效的文件 - {文件路径}")
        return None
    # 获取文件信息
    file_name = os.path.basename(文件路径)
    file_size = os.path.getsize(文件路径)
    print(f"准备上传文件: {file_name} (大小: {file_size} bytes)")
    # 检查文件大小限制（飞书直接上传限制20MB）
    if file_size > 20 * 1024 * 1024:
        print(f"错误：文件过大，超过20MB限制")
        return None
    # 创建client
    client = lark.Client.builder() \
        .app_id(应用ID) \
        .app_secret(应用密匙) \
        .log_level(lark.LogLevel.DEBUG) \
        .build()
    try:
        # 打开文件
        with open(文件路径, "rb") as file:
            # 构造请求对象
            request: UploadAllMediaRequest = UploadAllMediaRequest.builder() \
                .request_body(UploadAllMediaRequestBody.builder()
                              .file_name(file_name)
                              .parent_type("bitable_file")  # 上传到多维表格
                              .parent_node(DWBG_TOKEN)     # 多维表格的app_token
                              .size(str(file_size))         # 文件大小，字符串类型
                              .file(file)                   # 文件二进制内容
                              .build()) \
                .build()

            # 发起请求
            response: UploadAllMediaResponse = client.drive.v1.media.upload_all(request)

            # 处理失败返回
            if not response.success():
                error_msg = f"文件上传失败 - 代码: {response.code}, 消息: {response.msg}, 日志ID: {response.get_log_id()}"
                print(error_msg)
                # 输出详细响应内容
                if response.raw and response.raw.content:
                    try:
                        resp_content = json.loads(response.raw.content)
                        print("详细响应内容:")
                        print(json.dumps(resp_content, indent=4, ensure_ascii=False))
                    except:
                        print("响应内容解析失败:", response.raw.content)
                return None
            else:
                # 处理成功结果
                print("文件上传成功!")
                print("返回数据:", lark.JSON.marshal(response.data, indent=4))
                return response.data.file_token
    except Exception as e:
        print(f"上传过程发生错误: {str(e)}")
        return None

def 新增飞书表格(应用ID, 应用密匙, DWBG_TOKEN, DWBG_TABLE_ID, 上传数据结构):
    # 创建client
    # 使用 访问令牌 需开启 token 配置, 并在 request_option 中配置 token
    client = lark.Client.builder() \
        .app_id(应用ID) \
        .app_secret(应用密匙) \
        .log_level(lark.LogLevel.DEBUG) \
        .build()

    # 构造请求对象
    request: CreateAppTableRecordRequest = CreateAppTableRecordRequest.builder() \
        .app_token(DWBG_TOKEN) \
        .table_id(DWBG_TABLE_ID) \
        .request_body(AppTableRecord.builder()
                      .fields(上传数据结构)
                      .build()) \
        .build()

    # 发起请求
    response: CreateAppTableRecordResponse = client.bitable.v1.app_table_record.create(request)

    # 处理失败返回
    if not response.success():
        lark.logger.error(
            f"client.bitable.v1.app_table_record.create failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}")
        return

    # 处理业务结果
    lark.logger.info(lark.JSON.marshal(response.data, indent=4))

def 更新飞书表格(应用ID, 应用密匙, DWBG_TOKEN, DWBG_TABLE_ID, 行ID, 上传数据结构):
    # 创建client
    client = lark.Client.builder() \
        .app_id(应用ID) \
        .app_secret(应用密匙) \
        .log_level(lark.LogLevel.DEBUG) \
        .build()

    # 构造请求对象
    request: UpdateAppTableRecordRequest = UpdateAppTableRecordRequest.builder() \
        .app_token(DWBG_TOKEN) \
        .table_id(DWBG_TABLE_ID) \
        .record_id(行ID) \
        .request_body(AppTableRecord.builder()
                      .fields(上传数据结构)
                      .build()) \
        .build()

    # 发起请求
    response: UpdateAppTableRecordResponse = client.bitable.v1.app_table_record.update(request)

    # 处理失败返回
    if not response.success():
        lark.logger.error(
            f"client.bitable.v1.app_table_record.update failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}")
        return

def 获取多维表格内容(tenant_access_token, app_token, table_id):
    """获取多维表格所有记录（增加详细错误处理）"""
    all_records = []
    page_token = ''
    has_more = True

    while has_more:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search"
        payload = json.dumps({
            "page_token": page_token,
            "page_size": 100
        })
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {tenant_access_token}'
        }

        try:
            response = requests.post(url, headers=headers, data=payload, timeout=10)
            response.raise_for_status()  # 抛出HTTP错误（如404、403）
            result = response.json()

            if result.get('code') != 0:
                # 详细错误信息
                error_details = {
                    "code": result.get('code'),
                    "msg": result.get('msg'),
                    "app_token": app_token,
                    "table_id": table_id,
                    "url": url
                }
                raise Exception(f"飞书API错误: {json.dumps(error_details, ensure_ascii=False)}")

            data = result.get('data', {})
            items = data.get('items', [])
            all_records.extend(items)
            has_more = data.get('has_more', False)
            page_token = data.get('page_token', '')

        except requests.exceptions.HTTPError as e:
            # HTTP状态码错误（如404表示表格不存在）
            raise Exception(f"HTTP请求错误: {str(e)}，URL: {url}，可能是app_token或table_id错误")
        except Exception as e:
            raise Exception(f"获取表格内容失败: {str(e)}")

    return all_records

def 获取多维表格中附件的链接(访问令牌, DWBG_TOKEN, DWBG_TABLE_ID, 行ID=None, 附件字段名="附件"):
    """
    提取多维表格指定行的附件原始URL（适配指定附件列名称）
    :param 访问令牌: 飞书应用访问令牌
    :param DWBG_TOKEN: 多维表格APP_TOKEN
    :param DWBG_TABLE_ID: 多维表格TABLE_ID
    :param 行ID: 目标行的record_id（必填，精准定位行）
    :param 附件字段名: 表格中附件列的名称（比如"上传附件"）
    :return: 列表[(url, name), ...]
    """
    # 1. 校验必填参数并去除空格
    if not 行ID:
        raise ValueError("❌ 行ID不能为空，请传入目标行的record_id")
    
    # 去除空格
    行ID = str(行ID).strip()
    附件字段名 = str(附件字段名).strip()
    
    print(f"🔍 搜索参数: 行ID=[{行ID}], 附件字段名=[{附件字段名}]")

    # 2. 构造请求参数（支持分页）
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{DWBG_TOKEN}/tables/{DWBG_TABLE_ID}/records/search"
    headers = {
        "Authorization": f"Bearer {访问令牌}",
        "Content-Type": "application/json"
    }
    request_data = {"page_size": 100, "page_token": ""}
    all_attachments = []

    # 3. 分页读取记录，精准定位目标行
    while True:
        try:
            resp = requests.post(url, headers=headers, json=request_data, timeout=15)
            resp.raise_for_status()  # 抛出HTTP异常（如401/403/500）
            result = resp.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f"❌ 请求接口失败: {str(e)}")

        # 校验接口返回码
        if result["code"] != 0:
            raise Exception(f"❌ 读取表格失败: {result['msg']} (code: {result['code']})")

        # 4. 遍历记录，匹配目标行ID
        target_record = None
        for record in result["data"]["items"]:
            record_id = record["record_id"].strip() if record["record_id"] else record["record_id"]
            if record_id == 行ID:  # 精准匹配行ID
                target_record = record
                break

        # 找到目标行，提取附件
        if target_record:
            fields = target_record.get("fields", {})
            
            # 调试：打印所有字段名，看看实际有哪些字段
            print(f"📊 行 [{行ID}] 的字段列表:")
            for field_name in fields.keys():
                print(f"  - '{field_name}'")
            
            # 尝试精确匹配字段名（去除空格）
            attachments = None
            for field_name in fields.keys():
                if field_name.strip() == 附件字段名:
                    attachments = fields.get(field_name, [])
                    print(f"✅ 找到匹配的字段名: '{field_name}' -> '{附件字段名}'")
                    break
            
            # 如果没找到精确匹配，尝试模糊匹配
            if attachments is None:
                for field_name in fields.keys():
                    if 附件字段名 in field_name or field_name in 附件字段名:
                        attachments = fields.get(field_name, [])
                        print(f"⚠️ 模糊匹配字段名: '{field_name}' -> '{附件字段名}'")
                        break
            
            # 如果还没找到，打印可用字段名供参考
            if attachments is None:
                print(f"❌ 未找到字段名 '{附件字段名}'，可用字段:")
                for field_name in fields.keys():
                    print(f"  '{field_name}'")
                raise Exception(f"❌ 行ID [{行ID}] 的「{附件字段名}」列不存在")

            if not attachments:
                print(f"⚠️ 行ID [{行ID}] 的「{附件字段名}」列无附件，但字段存在")
                # 返回空列表而不是抛出异常
                return all_attachments

            # 筛选Excel格式附件
            for att in attachments:
                att_url = att.get("url")
                att_name = att.get("name", "")
                if att_url and att_name.endswith((".xlsx", ".xls")):
                    print(f"✅ 行ID [{行ID}] 提取到附件: {att_name} | URL: {att_url[:50]}...")
                    all_attachments.append((att_url, att_name))

            # 找到目标行后无需继续分页
            break

        # 5. 处理分页（无下一页则终止）
        if not result["data"].get("has_more"):
            break
        request_data["page_token"] = result["data"]["page_token"]

    # 6. 结果校验与返回
    if not all_attachments:
        print(f"⚠️ 行ID [{行ID}] 的「{附件字段名}」列未找到Excel附件")
        # 不抛出异常，返回空列表让程序继续执行
        return all_attachments

    return all_attachments  # 多附件时返回列表

def 在线解析表格为二维数据(访问令牌, 文件临时链接, 文件名称):
    """
    纯Python方案：手动清理Excel XML中的id属性 + pandas解析
    无任何外部依赖（除pandas/openpyxl），适配所有环境
    """
    import tempfile
    import os
    import zipfile
    import xml.etree.ElementTree as ET

    # 前置校验
    if not all([访问令牌, 文件临时链接, 文件名称]):
        print("❌ 解析参数为空")
        return None

    # 1. 下载文件到临时目录
    headers = {"Authorization": f"Bearer {访问令牌}"}
    try:
        temp_dir = tempfile.mkdtemp()
        raw_file = os.path.join(temp_dir, 文件名称)
        resp = requests.get(文件临时链接, headers=headers, timeout=300)
        resp.raise_for_status()
        with open(raw_file, 'wb') as f:
            f.write(resp.content)
        print(f"✅ 原始文件保存: {raw_file}")
    except Exception as e:
        print(f"❌ 下载失败: {str(e)}")
        return None

    # 2. 手动清理Excel中的id属性（核心修复）
    try:
        # .xlsx本质是zip包，解压后修改XML
        fixed_file = os.path.join(temp_dir, f"fixed_{文件名称}")

        # 解压原始Excel
        with zipfile.ZipFile(raw_file, 'r') as zip_in:
            with zipfile.ZipFile(fixed_file, 'w') as zip_out:
                # 遍历所有文件
                for item in zip_in.infolist():
                    data = zip_in.read(item.filename)

                    # 只处理工作表的XML文件（xl/worksheets/sheet*.xml）
                    if item.filename.startswith('xl/worksheets/') and item.filename.endswith('.xml'):
                        # 解析XML，删除所有id属性
                        root = ET.fromstring(data)
                        # 递归删除所有元素的id属性
                        def remove_id_attr(element):
                            if 'id' in element.attrib:
                                del element.attrib['id']
                            for child in element:
                                remove_id_attr(child)
                        remove_id_attr(root)
                        # 重新生成XML数据
                        data = ET.tostring(root, encoding='utf-8')

                    # 写入修复后的文件
                    zip_out.writestr(item, data)

        print(f"✅ 已清理Excel中的id属性，修复后文件: {fixed_file}")

    except Exception as e:
        print(f"❌ 清理id属性失败: {str(e)}")
        return None

    # 3. 用pandas解析修复后的文件
    try:
        工作表字典 = {}
        df_dict = pd.read_excel(
            fixed_file,
            engine="openpyxl",
            sheet_name=None,
            header=None
        )

        # 转换为二维列表
        import numpy as np
        for sheet_name, df in df_dict.items():
            df = df.fillna("")
            二维列表 = df.values.tolist()
            二维列表 = [
                [
                    str(cell) if isinstance(cell, (np.integer, np.floating, np.bool_))
                    else cell for cell in row
                ] for row in 二维列表
            ]
            工作表字典[sheet_name] = 二维列表

        print(f"✅ 解析完成，共{len(工作表字典)}个Sheet")

        # 清理临时文件
        import shutil
        shutil.rmtree(temp_dir)

        return 工作表字典

    except Exception as e:
        print(f"❌ pandas解析失败: {str(e)}")
        import traceback
        print(f"📝 详细错误: {traceback.format_exc()}")
        try:
            shutil.rmtree(temp_dir)
        except:
            pass
        return None

def 根据单元格内容提取行数列数(工作表内容, 搜索值: str):
    for 行数, 一行内容 in enumerate(工作表内容):
        for 列数, 单元格内容 in enumerate(一行内容):
            if 单元格内容:
                if 搜索值 == str(单元格内容):
                    return 行数, 列数
    return None, None

def 转换时间戳(input_var, timezone_offset=8):
    """
    将输入变量转换为毫秒级时间戳
    
    参数:
    input_var: 输入变量，可以是字符串、datetime对象或整数（Excel日期序列号）
    timezone_offset: 时区偏移量（小时），默认为8（UTC+8）
    
    返回:
    int: 毫秒级时间戳
    """
    import pandas as pd
    from datetime import datetime
    
    # 判断输入类型
    if isinstance(input_var, str):
        # 如果是字符串，尝试解析为datetime对象
        try:
            # 尝试常见日期格式
            dt = datetime.strptime(input_var, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                # 尝试其他可能格式
                dt = datetime.strptime(input_var, "%Y/%m/%d %H:%M:%S")
            except ValueError:
                try:
                    # 尝试ISO格式
                    dt = datetime.fromisoformat(input_var.replace('Z', '+00:00'))
                except ValueError:
                    try:
                        # 尝试只包含日期的格式
                        dt = datetime.strptime(input_var, "%Y-%m-%d")
                    except ValueError:
                        try:
                            # 尝试中文日期格式
                            dt = datetime.strptime(input_var, "%Y年%m月%d日")
                        except ValueError:
                            # 尝试处理可能带有多余空格的日期
                            input_var_clean = input_var.strip()
                            # 尝试多种分割符
                            for sep in ['-', '/', '.', '年', '月', '日']:
                                if sep in input_var_clean:
                                    parts = input_var_clean.replace('年', '-').replace('月', '-').replace('日', '').split('-')
                                    if len(parts) == 3:
                                        year, month, day = parts
                                        dt = datetime(int(year), int(month), int(day))
                                        break
                            else:
                                raise ValueError(f"无法解析日期字符串: {input_var}")

        # 计算时间戳（假设原始时间为指定时区）
        timestamp = dt.timestamp() + timezone_offset * 3600
        return int(timestamp * 1000)

    elif isinstance(input_var, datetime):
        # 如果是datetime对象，直接转换为时间戳
        return int((input_var.timestamp() + timezone_offset * 3600) * 1000)
    
    elif isinstance(input_var, (int, float)):
        # 如果是整数或浮点数，可能是Excel日期序列号或时间戳
        try:
            # 首先检查是否是Excel日期序列号（通常范围在40000-50000之间）
            # Excel日期序列号起点：1899-12-30
            if 20000 < input_var < 100000:  # 合理的Excel日期序列号范围
                # 处理Excel日期序列号
                excel_epoch = datetime(1899, 12, 30)
                
                # 计算天数和小数部分
                if isinstance(input_var, int):
                    days = input_var
                    seconds = 0
                else:
                    # 浮点数，整数部分是天数，小数部分是时间
                    days = int(input_var)
                    fraction = input_var - days
                    seconds = int(fraction * 86400)  # 一天86400秒
                
                dt = excel_epoch + pd.Timedelta(days=days, seconds=seconds)
                
                # 计算时间戳
                timestamp = dt.timestamp() + timezone_offset * 3600
                return int(timestamp * 1000)
            else:
                # 如果不是Excel日期序列号，尝试作为时间戳处理
                # 如果数值很大，可能是毫秒级时间戳
                if input_var > 1000000000000:  # 大于2001年
                    return int(input_var)  # 直接返回，假设已经是毫秒级时间戳
                elif input_var > 1000000000:  # 大于2001年，但小于1000000000000
                    return int(input_var * 1000)  # 假设是秒级时间戳，转换为毫秒级
                else:
                    # 数值太小，无法确定是什么
                    raise ValueError(f"无法确定数字 {input_var} 的日期格式")
        except Exception as e:
            print(f"❌ 数字类型日期转换失败: {input_var}, 错误: {str(e)}")
            # 尝试最后的手段：假设是自1970-01-01以来的天数
            try:
                dt = datetime(1970, 1, 1) + pd.Timedelta(days=input_var)
                timestamp = dt.timestamp() + timezone_offset * 3600
                return int(timestamp * 1000)
            except Exception as e2:
                raise ValueError(f"无法处理数字类型日期: {input_var}, 错误: {str(e2)}")

    else:
        raise TypeError(f"不支持的类型: {type(input_var)}. 支持字符串、datetime对象或整数/浮点数")

def 日期单元格转变(批次):
    if isinstance(批次, datetime):
        # 处理 datetime 对象
        return 批次.strftime("%Y-%m-%d")
    elif isinstance(批次, str):
        # 处理字符串格式 "YYYY-MM-DD HH:MM:SS"
        try:
            # 方法1：直接操作字符串（更快）
            return 批次[:4] + "-" + 批次[5:7] + "-" + 批次[8:10]
        except:
            # 处理格式错误的情况
            return "格式错误"
    else:
        # 处理其他类型（如时间戳）
        try:
            return datetime.fromtimestamp(批次).strftime("%Y%m%d")
        except:
            return "不支持的格式"

def 取表格标题(工作表内容: list, 第几行开始: int):
    行列标题字典 = {}
    if 工作表内容 is not None and 第几行开始 > 0:
        for 行数, 一行内容 in enumerate(工作表内容):
            if 行数 == 第几行开始 - 1:  # 取前六列的标题
                for 列数, 列内容 in enumerate(一行内容):
                    if 列内容 is not None:
                        行列标题字典.setdefault(列内容, [行数 + 1, 列数])
        return 行列标题字典

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

if __name__ == "__main__":
    数据字典 = {}

    # 从环境变量读取配置并去除空格
    APP_ID = os.getenv("APP_ID", "").strip()
    APP_SECRET = os.getenv("APP_SECRET", "").strip()
    DWBG_TOKEN = os.getenv("DWBG_TOKEN", "").strip()
    DWBG_TABLE_ID = os.getenv("DWBG_TABLE_ID", "").strip()
    
    # 尝试多种可能的变量名，包括中文和英文
    ROW_ID = (os.getenv("ROW_ID") or os.getenv("行ID") or "").strip()
    
    # 尝试多种可能的变量名
    QSA_TABLE_ID = (os.getenv("QSA_TABLE_ID") or os.getenv("失分点填入_TABLE_ID") or os.getenv("失分点填入TABLE_ID") or "").strip()
    
    FJ_ID = (os.getenv("FJ_ID") or os.getenv("附件字段名") or "").strip()
    
    # 调试：打印所有环境变量（调试用）
    print("=== 完整环境变量调试 ===")
    print("所有环境变量:")
    for key, value in os.environ.items():
        if key in ["APP_ID", "APP_SECRET", "DWBG_TOKEN", "DWBG_TABLE_ID", 
                  "ROW_ID", "行ID", "QSA_TABLE_ID", "失分点填入_TABLE_ID", "失分点填入TABLE_ID",
                  "FJ_ID", "附件字段名"]:
            print(f"  {key}: '{value}'")
    
    print("\n=== 使用的环境变量（去除空格后） ===")
    print(f"APP_ID: '{APP_ID}'")
    print(f"APP_SECRET: {'已设置' if APP_SECRET else '未设置'}")
    print(f"DWBG_TOKEN: '{DWBG_TOKEN}'")
    print(f"DWBG_TABLE_ID: '{DWBG_TABLE_ID}'")
    print(f"ROW_ID: '{ROW_ID}'")
    print(f"QSA_TABLE_ID: '{QSA_TABLE_ID}'")
    print(f"FJ_ID: '{FJ_ID}'")
    
    # 如果QSA_TABLE_ID未设置，尝试从其他来源获取
    if not QSA_TABLE_ID:
        print("⚠️ QSA_TABLE_ID未设置，尝试使用默认值...")
        # 这里可以设置一个默认值，或者从其他途径获取
        # 暂时先设置为空，后续再处理
        pass
    
    # 校验配置（修改变量名，允许QSA_TABLE_ID为空时的处理）
    if not all([APP_ID, APP_SECRET, DWBG_TOKEN, DWBG_TABLE_ID, FJ_ID, ROW_ID]):
        missing_vars = []
        if not APP_ID: missing_vars.append("APP_ID")
        if not APP_SECRET: missing_vars.append("APP_SECRET")
        if not DWBG_TOKEN: missing_vars.append("DWBG_TOKEN")
        if not DWBG_TABLE_ID: missing_vars.append("DWBG_TABLE_ID")
        if not FJ_ID: missing_vars.append("FJ_ID")
        if not ROW_ID: missing_vars.append("ROW_ID")
        raise Exception(f"❌ 环境变量配置不完整，缺少: {', '.join(missing_vars)}")
    
    # 如果QSA_TABLE_ID未设置，我们可能无法继续执行
    if not QSA_TABLE_ID:
        print("⚠️ 警告: QSA_TABLE_ID未设置，将无法创建失分点记录")
        print("   程序将继续执行，但只会更新主表，不会创建失分点记录")

    '''第一步先获取多维表格数据'''
    访问令牌 = 获取访问令牌(APP_ID, APP_SECRET)
    print(f"访问令牌: {访问令牌[:50] if 访问令牌 else '获取失败'}...")
    
    # 修改函数调用参数（使用英文变量名）
    获取信息 = 获取多维表格中附件的链接(访问令牌, DWBG_TOKEN, DWBG_TABLE_ID, ROW_ID, FJ_ID)
    
    # 检查是否获取到附件
    if not 获取信息:
        print("⚠️ 未找到附件，程序将继续执行但无法处理数据")
    else:
        print(f"✅ 找到 {len(获取信息)} 个附件")

    数据字典 = {}

    for 列表元素_元组 in 获取信息:
        文件临时链接 = 列表元素_元组[0]
        文件名称 = 列表元素_元组[1]
        print(f"处理附件: {文件名称}")
        读取数据字典 = 在线解析表格为二维数据(访问令牌, 文件临时链接, 文件名称)
        if 读取数据字典:
            结果列表 = []
            for 工作表名称, 工作表内容 in 读取数据字典.items():
                print(f"处理工作表: {工作表名称}")
                if "汇总" in 工作表名称 or "新增章节" in 工作表名称:
                    搜索列表 = ["工厂名称：", "审核员姓名：", "审核开始日期：", "审核结束日期：", "得分"]
                    for 计次, 列表元素 in enumerate(搜索列表):
                        行列列表 = 根据单元格内容提取行数列数(工作表内容, 列表元素)
                        if 行列列表[0] is not None and 行列列表[1] is not None:
                            单元格内容 = 工作表内容[行列列表[0]][行列列表[1] + 2]
                            if 单元格内容:
                                if 计次 == 0:
                                    单元格内容 = 新检查工厂字典.get(单元格内容)
                                    结果列表.append(单元格内容)
                                elif 计次 == 1:
                                    结果列表.append(单元格内容)
                                elif 计次 == 2 or 计次 == 3:
                                    单元格内容 = 日期单元格转变(单元格内容)
                                    结果列表.append(单元格内容)
                                elif 计次 == 4:
                                    try:
                                        单元格内容 = round(float(单元格内容) * 100, 2)
                                        结果列表.append(单元格内容)
                                    except (ValueError, TypeError):
                                        print(f"❌ 得分格式错误: {单元格内容}")
                                        结果列表.append(0)
                            else:
                                print(f"❌ 未找到{列表元素}对应的内容")
                                if 计次 == 4:  # 如果是得分，设置为0
                                    结果列表.append(0)
                                else:
                                    结果列表.append("")
                        else:
                            print(f"❌ 未找到{列表元素}")
                            if 计次 == 4:  # 如果是得分，设置为0
                                结果列表.append(0)
                            else:
                                结果列表.append("")

                elif "检查表" in 工作表名称:
                    # 结果列表 =['光泽三厂', '全涛', '2025-11-17', '2025-11-20', 84.38]
                    if len(结果列表) >= 4 and all(isinstance(x, str) for x in 结果列表[2:4]):
                        结果列表[2:4] = [f'{结果列表[2]}~{结果列表[3]}']
                        print(f"结果列表: {结果列表}")
                        标题字典 = 取表格标题(工作表内容, 1)
                        符合级别行列列表 = 标题字典.get("符合级别")
                        if 符合级别行列列表:
                            for 行数, 一行内容 in enumerate(工作表内容):
                                if 行数 > 0 and len(一行内容) > 符合级别行列列表[1] and 一行内容[符合级别行列列表[1]]:
                                    符合等级 = str(一行内容[符合级别行列列表[1]]).strip()
                                    if "S" in 符合等级 or "P" in 符合等级 or "s" in 符合等级 or "p" in 符合等级:
                                        扣分等级 = 符合等级
                                        审核条款 = 一行内容[符合级别行列列表[1] - 2] if len(一行内容) > 符合级别行列列表[1] - 2 else ""
                                        条款标准 = 一行内容[符合级别行列列表[1] - 1] if len(一行内容) > 符合级别行列列表[1] - 1 else ""
                                        问题描述 = 一行内容[符合级别行列列表[1] + 1] if len(一行内容) > 符合级别行列列表[1] + 1 else ""
                                        根因分析 = 一行内容[符合级别行列列表[1] + 2] if len(一行内容) > 符合级别行列列表[1] + 2 else ""
                                        改进计划 = 一行内容[符合级别行列列表[1] + 3] if len(一行内容) > 符合级别行列列表[1] + 3 else ""
                                        计划完成期限 = 一行内容[符合级别行列列表[1] + 4] if len(一行内容) > 符合级别行列列表[1] + 4 else ""
                                        
                                        # 根据FJ_ID判断审核项类型
                                        if FJ_ID and "QSA+" in FJ_ID.upper():
                                            审核项 = "QSA+"
                                        else:
                                            审核项 = "QSA"
                                        
                                        # 构建数据结构
                                        取值列表 = 数据字典.setdefault(结果列表[0], {}).setdefault(结果列表[1], {}).setdefault(结果列表[2],{}).setdefault(
                                            审核项,{}).setdefault(
                                            结果列表[3], {}).setdefault(审核条款, {}).setdefault(条款标准, {}).setdefault(扣分等级,{}).setdefault(
                                            根因分析, {}).setdefault(改进计划, {}).setdefault(计划完成期限,问题描述)
                                        print(f"  发现失分点: {审核条款} - {扣分等级}")
                    else:
                        print(f"结果列表获取数据不足: {结果列表}")

    审核成绩上传数据结构 = {}
    审核失分点上传数据结构 = {}

    for 工厂名称, 嵌入字典 in 数据字典.items():
        审核成绩上传数据结构.setdefault("工厂名称", 工厂名称)
        for 审核人名称, 嵌入字典2 in 嵌入字典.items():
            审核成绩上传数据结构.setdefault("审核员", 审核人名称)
            for 审核日期, 嵌入字典3 in 嵌入字典2.items():

                分割内容 = str(审核日期).split("~")

                if len(分割内容) == 2:
                    审核开始时间戳 = 转换时间戳(分割内容[0])
                    审核成绩上传数据结构.setdefault("审核开始日期", 审核开始时间戳)
                    print(f"审核开始时间戳: {审核开始时间戳}")

                    审核结束时间戳 = 转换时间戳(分割内容[1])
                    审核成绩上传数据结构.setdefault("审核结束日期", 审核结束时间戳)
                    print(f"审核结束时间戳: {审核结束时间戳}")
                else:
                    print(f"❌ 审核日期格式错误: {审核日期}")

                for 审核项, 嵌入字典4 in 嵌入字典3.items():
                    for 审核得分, 嵌入字典5 in 嵌入字典4.items():
                        if "QSA+" == 审核项:
                            审核成绩上传数据结构.setdefault("QSA+得分", 审核得分)
                        else:
                            审核成绩上传数据结构.setdefault("QSA得分", 审核得分)
                        
                        if 审核成绩上传数据结构:
                            print(f"更新主表数据: {审核成绩上传数据结构}")
                            更新飞书表格(APP_ID, APP_SECRET, DWBG_TOKEN, DWBG_TABLE_ID, ROW_ID, 审核成绩上传数据结构)
                            审核成绩上传数据结构 = {}

                        for 审核条款, 嵌入字典6 in 嵌入字典5.items():
                            for 条款标准, 嵌入字典7 in 嵌入字典6.items():
                                for 扣分等级, 嵌入字典8 in 嵌入字典7.items():
                                    for 根因分析, 嵌入字典9 in 嵌入字典8.items():
                                        for 改进计划, 嵌入字典10 in 嵌入字典9.items():
                                            for 计划完成期限, 问题明细 in 嵌入字典10.items():
                                                try:
                                                    # 打印调试信息
                                                    print(f"处理计划完成期限: {计划完成期限}, 类型: {type(计划完成期限)}")
                                                    
                                                    # 转换时间戳
                                                    计划转化后时间戳 = 转换时间戳(计划完成期限)
                                                    print(f"转换后的时间戳: {计划转化后时间戳}")
                                                    if 计划转化后时间戳:
                                                        审核失分点上传数据结构["计划完成时限"] = 计划转化后时间戳
                                                    
                                                    审核失分点上传数据结构.setdefault("工厂名称", 工厂名称)
                                                    审核失分点上传数据结构.setdefault("审核员", 审核人名称)
                                                    审核失分点上传数据结构.setdefault("审核日期", 审核日期)
                                                    审核失分点上传数据结构.setdefault("审核项", 审核项)
                                                    审核失分点上传数据结构["审核条款"] = 审核条款
                                                    审核失分点上传数据结构["审核标准"] = 条款标准
                                                    审核失分点上传数据结构["符合等级"] = 扣分等级
                                                    审核失分点上传数据结构["根因分析"] = 根因分析
                                                    审核失分点上传数据结构["改进计划"] = 改进计划
                                                    审核失分点上传数据结构["问题描述"] = 问题明细

                                                    if 审核失分点上传数据结构 and QSA_TABLE_ID:
                                                        print(f"创建失分点记录: {审核失分点上传数据结构}")
                                                        新增飞书表格(APP_ID, APP_SECRET, DWBG_TOKEN, QSA_TABLE_ID, 审核失分点上传数据结构)
                                                        # 清空数据结构
                                                        审核失分点上传数据结构 = {}
                                                    elif not QSA_TABLE_ID:
                                                        print("⚠️ 跳过创建失分点记录: QSA_TABLE_ID未设置")
                                                except Exception as e:
                                                    print(f"❌ 处理计划完成期限失败: {计划完成期限}, 错误: {str(e)}")
                                                    print(f"   跳过此记录，继续处理下一个...")
                                                    # 继续处理下一个记录
                                                    continue
