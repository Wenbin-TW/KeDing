import os
base_path = '/Users/alysonchen/Downloads/KeDing'
output_file = os.path.join(base_path, 'ALL_README_SUMMARY.md')

def merge_readmes():
    merged_content = []
    for root, dirs, files in os.walk(base_path):
        if root == base_path:
            continue
        readme_files = [f for f in files if f.lower().startswith('readme')]
        
        if not readme_files:
            continue
        md_files = [f for f in readme_files if f.lower().endswith('.md')]
        target_file = md_files[0] if md_files else readme_files[0]
        
        file_path = os.path.join(root, target_file)
        project_name = os.path.relpath(root, base_path)

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            section = f"## 📂 项目路径: {project_name}\n"
            section += f"> **文件来源:** `{file_path}`\n\n"
            section += content
            section += "\n\n---\n\n" # 分隔符
            
            merged_content.append(section)
            print(f"✅ 已添加: {project_name}")
            
        except Exception as e:
            print(f"❌ 无法读取 {file_path}: {e}")
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"# 汇总文档 - {os.path.basename(base_path)}\n\n")
        f.writelines(merged_content)

    print(f"\n✨ 完成！汇总文件已生成至: {output_file}")

if __name__ == "__main__":
    merge_readmes()