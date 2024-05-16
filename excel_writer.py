import pandas as pd

def excel_writer(file_name: str, df: pd.DataFrame) -> None:
    #create excel writer
    writer = pd.ExcelWriter(f"D:\Python\Python_projects\Bibance_API\Database\{file_name}.xlsx")
    # write dataframe to excel sheet named 'marks'
    df.to_excel(writer, f'{file_name}')
    # save the excel file
    writer.close()
    print(f'DataFrame {file_name} успешно сохранён.')