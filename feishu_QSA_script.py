'''飞书多维表格需要的库'''
import os
import json
from datetime import datetime
import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *
from lark_oapi.api.drive.v1 import *
import requests
import datetime
import pyexcel
import pandas as pd
import io

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

# ... 其他函数保持不变 ...

if __name__ == "__main__":
    # ... 前面的代码保持不变 ...
    
    审核成绩上传数据结构 = {}
    审核失分点上传数据结构 = {}

    for 工厂名称, 嵌入字典 in 数据字典.items():
        审核成绩上传数据结构.setdefault("工厂名称",工厂名称)
        for 审核人名称, 嵌入字典2 in 嵌入字典.items():
            审核成绩上传数据结构.setdefault("审核员",审核人名称)
            for 审核日期, 嵌入字典3 in 嵌入字典2.items():

                分割内容 = str(审核日期).split("~")

                if len(分割内容) == 2:
                    审核开始时间戳 = 转换时间戳(分割内容[0])
                    审核成绩上传数据结构.setdefault("审核开始日期",审核开始时间戳)
                    print(f"审核开始时间戳: {审核开始时间戳}")

                    审核结束时间戳 = 转换时间戳(分割内容[1])
                    审核成绩上传数据结构.setdefault("审核结束日期",审核结束时间戳)
                    print(f"审核结束时间戳: {审核结束时间戳}")
                else:
                    print(f"❌ 审核日期格式错误: {审核日期}")

                for 审核项, 嵌入字典4 in 嵌入字典3.items():
                    for 审核得分, 嵌入字典5 in 嵌入字典4.items():
                        if "QSA+" == 审核项:
                            审核成绩上传数据结构.setdefault("QSA+得分",审核得分)
                        else:
                            审核成绩上传数据结构.setdefault("QSA得分",审核得分)
                        
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
                                                    
                                                    审核失分点上传数据结构.setdefault("工厂名称", 工厂名称)
                                                    审核失分点上传数据结构.setdefault("审核员", 审核人名称)
                                                    审核失分点上传数据结构.setdefault("审核日期", 审核日期)
                                                    审核失分点上传数据结构.setdefault("审核项", 审核项)
                                                    审核失分点上传数据结构["审核条款"] = 审核条款
                                                    审核失分点上传数据结构["审核标准"] = 条款标准
                                                    审核失分点上传数据结构["符合等级"] = 扣分等级
                                                    审核失分点上传数据结构["根因分析"] = 根因分析
                                                    审核失分点上传数据结构["改进计划"] = 改进计划
                                                    审核失分点上传数据结构["计划完成时限"] = 计划转化后时间戳
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
