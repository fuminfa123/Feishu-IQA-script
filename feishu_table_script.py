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
import traceback

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
    """使用飞书官方SDK上传文件到多维表格（本脚本未使用，保留兼容）"""
    if not os.path.exists(文件路径):
        print(f"错误：文件不存在 - {文件路径}")
        return None
    if not os.path.isfile(文件路径):
        print(f"错误：不是有效的文件 - {文件路径}")
        return None
    file_name = os.path.basename(文件路径)
    file_size = os.path.getsize(文件路径)
    print(f"准备上传文件: {file_name} (大小: {file_size} bytes)")
    if file_size > 20 * 1024 * 1024:
        print(f"错误：文件过大，超过20MB限制")
        return None
    client = lark.Client.builder() \
        .app_id(应用ID) \
        .app_secret(应用密匙) \
        .log_level(lark.LogLevel.DEBUG) \
        .build()
    try:
        with open(文件路径, "rb") as file:
            request: UploadAllMediaRequest = UploadAllMediaRequest.builder() \
                .request_body(UploadAllMediaRequestBody.builder()
                              .file_name(file_name)
                              .parent_type("bitable_file")
                              .parent_node(DWBG_TOKEN)
                              .size(str(file_size))
                              .file(file)
                              .build()) \
                .build()
            response: UploadAllMediaResponse = client.drive.v1.media.upload_all(request)
            if not response.success():
                error_msg = f"文件上传失败 - 代码: {response.code}, 消息: {response.msg}, 日志ID: {response.get_log_id()}"
                print(error_msg)
                if response.raw and response.raw.content:
                    try:
                        resp_content = json.loads(response.raw.content)
                        print("详细响应内容:", json.dumps(resp_content, indent=4, ensure_ascii=False))
                    except:
                        print("响应内容解析失败:", response.raw.content)
                return None
            else:
                print("文件上传成功!")
                print("返回数据:", lark.JSON.marshal(response.data, indent=4))
                return response.data.file_token
    except Exception as e:
        print(f"上传过程发生错误: {str(e)}")
        return None

def 新增飞书表格(应用ID, 应用密匙, DWBG_TOKEN, DWBG_TABLE_ID, 上传数据结构):
    """新增飞书表格记录"""
    client = lark.Client.builder() \
        .app_id(应用ID) \
        .app_secret(应用密匙) \
        .log_level(lark.LogLevel.DEBUG) \
        .build()
    request: CreateAppTableRecordRequest = CreateAppTableRecordRequest.builder() \
        .app_token(DWBG_TOKEN) \
        .table_id(DWBG_TABLE_ID) \
        .request_body(AppTableRecord.builder()
                      .fields(上传数据结构)
                      .build()) \
        .build()
    response: CreateAppTableRecordResponse = client.bitable.v1.app_table_record.create(request)
    if not response.success():
        error_info = f"新增记录失败 - 代码: {response.code}, 消息: {response.msg}, 日志ID: {response.get_log_id()}"
        if response.raw and response.raw.content:
            try:
                resp_content = json.loads(response.raw.content)
                error_info += f"\n详细响应: {json.dumps(resp_content, indent=4, ensure_ascii=False)}"
            except:
                error_info += f"\n响应内容: {response.raw.content}"
        print(error_info)
        return False
    else:
        print("新增记录成功:", lark.JSON.marshal(response.data, indent=4))
        return True

def 更新飞书表格(应用ID, 应用密匙, DWBG_TOKEN, DWBG_TABLE_ID, 行ID, 上传数据结构):
    """更新飞书表格记录（本脚本未使用，保留兼容）"""
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
        error_info = f"更新记录失败 - 代码: {response.code}, 消息: {response.msg}, 日志ID: {response.get_log_id()}"
        if response.raw and response.raw.content:
            try:
                resp_content = json.loads(response.raw.content)
                error_info += f"\n详细响应: {json.dumps(resp_content, indent=4, ensure_ascii=False)}"
            except:
                error_info += f"\n响应内容: {response.raw.content}"
        print(error_info)
        return False
    else:
        print("更新记录成功")
        return True

def 获取多维表格内容(tenant_access_token, app_token, table_id):
    """获取多维表格所有记录"""
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
            response.raise_for_status()
            result = response.json()
            if result.get('code') != 0:
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
            raise Exception(f"HTTP请求错误: {str(e)}，URL: {url}，可能是app_token或table_id错误")
        except Exception as e:
            raise Exception(f"获取表格内容失败: {str(e)}")
    return all_records

def 获取多维表格中附件的链接(访问令牌, DWBG_TOKEN, DWBG_TABLE_ID, 行ID, 附件字段名="附件"):
    """提取多维表格指定行的附件链接"""
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
    """在线解析Excel表格为二维数据"""
    if not all([访问令牌, 文件临时链接, 文件名称]):
        print("❌ 解析参数为空")
        return None
    import tempfile
    import zipfile
    import xml.etree.ElementTree as ET
    import shutil
    try:
        temp_dir = tempfile.mkdtemp()
        raw_file = os.path.join(temp_dir, 文件名称)
        headers = {"Authorization": f"Bearer {访问令牌}"}
        resp = requests.get(文件临时链接, headers=headers, timeout=300)
        resp.raise_for_status()
        with open(raw_file, 'wb') as f:
            f.write(resp.content)
        print(f"✅ 原始文件保存: {raw_file}")
        fixed_file = os.path.join(temp_dir, f"fixed_{文件名称}")
        with zipfile.ZipFile(raw_file, 'r') as zip_in:
            with zipfile.ZipFile(fixed_file, 'w') as zip_out:
                for item in zip_in.infolist():
                    data = zip_in.read(item.filename)
                    if item.filename.startswith('xl/worksheets/') and item.filename.endswith('.xml'):
                        root = ET.fromstring(data)
                        def remove_id_attr(element):
                            if 'id' in element.attrib:
                                del element.attrib['id']
                            for child in element:
                                remove_id_attr(child)
                        remove_id_attr(root)
                        data = ET.tostring(root, encoding='utf-8')
                    zip_out.writestr(item, data)
        print(f"✅ 已清理Excel中的id属性，修复后文件: {fixed_file}")
        工作表字典 = {}
        df_dict = pd.read_excel(
            fixed_file,
            engine="openpyxl",
            sheet_name=None,
            header=None
        )
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
        shutil.rmtree(temp_dir)
        return 工作表字典
    except Exception as e:
        print(f"❌ 解析表格失败: {str(e)}")
        print(f"📝 详细错误: {traceback.format_exc()}")
        try:
            shutil.rmtree(temp_dir)
        except:
            pass
        return None

def 根据单元格内容提取行数列数(工作表内容, 搜索值: str):
    """根据单元格内容查找行数列数"""
    for 行数, 一行内容 in enumerate(工作表内容):
        for 列数, 单元格内容 in enumerate(一行内容):
            if 单元格内容 and 搜索值 == str(单元格内容):
                return 行数, 列数
    return None, None

def 获取单重数据(工作表内容, 参数1):
    """提取单重数据"""
    数据开始行数, 数据开始列数 = 根据单元格内容提取行数列数(工作表内容, "第1个")
    if not 数据开始行数:
        return None, None, None, None, None, None
    try:
        if 参数1 == "蒸煮后" or 参数1 == "蒸后":
            品名 = 工作表内容[0][2]
            工艺单 = 工作表内容[1][2]
            工序 = 工作表内容[2][2]
            标准下限 = 工作表内容[3][2]
            标准上限 = 工作表内容[3][3]
            每组数列 = 工作表内容[4][2]
            监控白班 = 工作表内容[3][4]
            监控夜班 = 工作表内容[5][4]
        elif "炸后" in 参数1 or "炭烤" in 参数1:
            品名 = 工作表内容[0][1]
            工艺单 = 工作表内容[1][1]
            工序 = 工作表内容[2][1]
            标准下限 = 工作表内容[3][1]
            标准上限 = 工作表内容[3][2]
            每组数列 = 工作表内容[4][1]
            监控白班 = 工作表内容[5][2]
            监控夜班 = 工作表内容[5][3]
        elif 参数1 == "一次包装" or 参数1 == "原料全检":
            品名 = 工作表内容[0][1]
            工艺单 = 工作表内容[1][1]
            工序 = 工作表内容[2][1]
            if 参数1 == "一次包装":
                标准下限 = 工作表内容[3][2]
                标准上限 = 工作表内容[3][3]
            else:
                标准下限 = 工作表内容[3][1]
                标准上限 = 工作表内容[3][2]
            每组数列 = 工作表内容[4][1]
            监控白班 = 工作表内容[7][3]
            监控夜班 = 工作表内容[8][3]
        else:
            return None, None, None, None, None, None
        if not 标准上限:
            标准上限 = 9999
        单重数据时间列表 = []
        for 行数, 一行内容 in enumerate(工作表内容):
            if 行数 == 7:
                for 列数, 列元素 in enumerate(一行内容):
                    if 7 <= 列数 <= 31 and 列元素:
                        单重数据时间列表.append(列元素)
        if not isinstance(每组数列, int):
            return None, None, None, None, None, None
        数据范围 = [数据开始行数, 3, 数据开始行数 + 每组数列, 30]
        列数范围 = list(range(2, 30))
        行数范围 = list(range(int(数据范围[0]), int(数据范围[2])))
        单重数据列表_二维数组 = []
        for 列数 in 列数范围:
            每组列表 = []
            for 片数, 行数 in enumerate(行数范围):
                单重 = 工作表内容[行数][列数]
                if 单重 and isinstance(单重, int):
                    每组列表.append(单重)
            if 每组列表:
                单重数据列表_二维数组.append(每组列表.copy())
        return 单重数据列表_二维数组, 单重数据时间列表, 标准下限, 标准上限, 品名, 工艺单
    except Exception as e:
        print(f"❌ 提取单重数据失败: {str(e)}")
        return None, None, None, None, None, None

def 转换时间戳(input_var, timezone_offset=8):
    """转换为毫秒级时间戳"""
    if isinstance(input_var, str):
        try:
            dt = datetime.strptime(input_var, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                dt = datetime.strptime(input_var, "%Y/%m/%d %H:%M:%S")
            except ValueError:
                try:
                    dt = datetime.fromisoformat(input_var.replace('Z', '+00:00'))
                except ValueError:
                    raise ValueError(f"无法解析日期字符串: {input_var}")
        timestamp = dt.timestamp() - timezone_offset * 3600
        return int(timestamp * 1000)
    elif isinstance(input_var, datetime):
        return int(input_var.timestamp() * 1000)
    else:
        raise TypeError(f"不支持的类型: {type(input_var)}. 只支持字符串或datetime对象")

def main():
    """主函数：处理飞书表格数据"""
    try:
        # 从环境变量读取配置
        APP_ID = os.getenv("APP_ID")
        APP_SECRET = os.getenv("APP_SECRET")
        DWBG_TOKEN = os.getenv("DWBG_TOKEN")
        DWBG_TABLE_ID = os.getenv("DWBG_TABLE_ID")
        ROW_ID = os.getenv("ROW_ID")
        TARGET_TABLE_ID = os.getenv("TARGET_TABLE_ID")
        
        # 校验配置
        if not all([APP_ID, APP_SECRET, DWBG_TOKEN, DWBG_TABLE_ID, ROW_ID, TARGET_TABLE_ID]):
            raise Exception("❌ 环境变量配置不完整，请检查Secrets和工作流配置")
        
        # 获取访问令牌
        访问令牌 = 获取访问令牌(APP_ID, APP_SECRET)
        print(f"✅ 获取访问令牌成功: {访问令牌[:20]}...")
        
        # 获取附件链接
        所有本地数据列表 = []
        获取信息 = 获取多维表格中附件的链接(访问令牌, DWBG_TOKEN, DWBG_TABLE_ID, ROW_ID, "上传附件")
        for 列表元素_元组 in 获取信息:
            文件临时链接, 文件名称 = 列表元素_元组
            print(f"📥 处理附件: {文件名称}")
            读取数据字典 = 在线解析表格为二维数据(访问令牌, 文件临时链接, 文件名称)
            if 读取数据字典:
                for 工作表名称, 工作表内容 in 读取数据字典.items():
                    工序行数, 工序列数 = 根据单元格内容提取行数列数(工作表内容, "工序")
                    if 工序行数:
                        工序获取值 = 工作表内容[工序行数][工序列数 + 1] if (工序列数 + 1) < len(工作表内容[工序行数]) else None
                        工序获取值2 = 工作表内容[工序行数][工序列数 + 2] if (工序列数 + 2) < len(工作表内容[工序行数]) else None
                        工序内容 = 工序获取值 or 工序获取值2 or None
                        if 工序内容:
                            单重数据信息, 单重数据时间列表, 标准下限, 标准上限, 品名, 工艺单 = 获取单重数据(工作表内容, 工序内容)
                            if 单重数据信息 and 单重数据时间列表:
                                for 计次, 列表元素_子元素 in enumerate(单重数据信息):
                                    if 计次 < len(单重数据时间列表):
                                        单重数据时间 = 单重数据时间列表[计次]
                                        if isinstance(列表元素_子元素, list):
                                            单重数据 = ",".join(map(str, 列表元素_子元素))
                                            所有本地数据列表.append([工序内容, 单重数据时间, 单重数据, 标准下限, 标准上限, 品名, 工艺单])
                                        else:
                                            print(f"⚠️ 非列表数据: {单重数据时间} - {列表元素_子元素}")
                                    else:
                                        print(f"⚠️ 数据索引超出时间列表长度: 计次{计次}")
                            else:
                                print(f"⚠️ 未提取到单重数据: {工作表名称}")
                        else:
                            print(f"⚠️ 未找到工序内容: {工作表名称}")
                    else:
                        print(f"⚠️ 未找到工序单元格: {工作表名称}")
            else:
                print(f"❌ 解析附件失败: {文件名称}")
        
        # 新增数据到飞书表格
        print(f"\n📊 共处理{len(所有本地数据列表)}条数据，开始写入飞书表格...")
        for 列表元素_子列表 in 所有本地数据列表:
            上传数据结构2 = {}
            字段名列表 = ["工序", "记录日期", "单重数据", "标准下限", "标准上限", "品名", "工艺单"]
            for 计次, 列表元素_子元素 in enumerate(列表元素_子列表):
                if 计次 >= len(字段名列表):
                    continue
                字段名 = 字段名列表[计次]
                if 计次 == 1:  # 记录日期转换为时间戳
                    try:
                        字段内容 = 转换时间戳(列表元素_子元素)
                    except Exception as e:
                        print(f"⚠️ 时间转换失败: {列表元素_子元素} - {str(e)}")
                        字段内容 = None
                else:
                    字段内容 = 列表元素_子元素
                if 字段名 and 字段内容 is not None:
                    上传数据结构2[字段名] = 字段内容
            if 上传数据结构2:
                print(f"📝 写入数据: {json.dumps(上传数据结构2, ensure_ascii=False)}")
                新增结果 = 新增飞书表格(APP_ID, APP_SECRET, DWBG_TOKEN, TARGET_TABLE_ID, 上传数据结构2)
                if not 新增结果:
                    print(f"❌ 写入数据失败: {上传数据结构2}")
            else:
                print(f"⚠️ 空数据结构，跳过写入")
        
        print("\n✅ 脚本执行完成")
    
    except Exception as e:
        print(f"\n❌ 脚本执行出错: {str(e)}")
        print(f"📝 详细错误栈: {traceback.format_exc()}")
        raise  # 抛出异常让GitHub Actions标记为失败

if __name__ == "__main__":
    main()
