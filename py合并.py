import os
import re

# ==========================================
# 🚩 在这里修改你的目标文件夹路径
# ==========================================
TARGET_DIR = '/Users/alysonchen/Downloads/KeDing/#拜訪清單'
# ==========================================

def get_file_sort_key(filename):
    """
    提取文件名开头的数字用于排序。
    如果没有数字，则返回一个很大的数，排在最后。
    """
    match = re.match(r'^(\d+)', filename)
    if match:
        return int(match.group(1))
    return 999  # 无数字的文件排在有数字的后面

def merge_scripts():
    if not os.path.exists(TARGET_DIR):
        print(f"❌ 错误：路径不存在 -> {TARGET_DIR}")
        return

    # 获取文件夹名称作为输出文件名
    folder_name = os.path.basename(os.path.normpath(TARGET_DIR))
    output_filename = f"{folder_name}_all.py"
    output_path = os.path.join(TARGET_DIR, output_filename)

    # 获取目录下所有 .py 文件
    all_files = [f for f in os.listdir(TARGET_DIR) 
                 if f.endswith('.py') and f != output_filename and f != 'merge_py_files.py']

    if not all_files:
        print("? 未找到可合并的 .py 文件")
        return

    # 按照文件名开头的数字排序 (1, 2, 3... 99)
    all_files.sort(key=get_file_sort_key)

    merged_content = []
    # 头部注释，标注生成信息
    merged_content.append(f'# -*- coding: utf-8 -*-\n# Generated from folder: {TARGET_DIR}\n\n')

    for file in all_files:
        file_path = os.path.join(TARGET_DIR, file)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # 添加分割线和来源标注，方便调试
            merged_content.append(f"# {'='*50}\n")
            merged_content.append(f"# SOURCE FILE: {file}\n")
            merged_content.append(f"# {'='*50}\n\n")
            merged_content.append(content)
            merged_content.append("\n\n") # 确保文件间有空行
            
            print(f"✅ 已加入: {file}")
        except Exception as e:
            print(f"❌ 读取 {file} 失败: {e}")

    # 写入新文件
    with open(output_path, 'w', encoding='utf-8') as f:
        f.writelines(merged_content)

    print(f"\n✨ 成功！合并后的文件位于: {output_path}")

if __name__ == "__main__":
    merge_scripts()