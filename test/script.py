from pathlib import Path

folder = Path.cwd()

for file in folder.iterdir():
    if file.is_file() and file.name != 'script.py':
        content = file.read_text()
        print(f"Content of {file.name} - {content}")

print("File reading done!")
