
def parse(data: bytes) -> list[tuple]:
    """Парсит XML-байты и возвращает список кортежей."""
    from lxml import etree

    root = etree.fromstring(data)
    data_list = root.xpath("//DirectoryEntry") 
    data = []
    for employe in data_list:
        tuple_data = (employe.find(".//Name").text,
                         employe.find(".//Telephone[@label='Email']").text,
                         employe.find(".//Telephone[@label='Work']").text,
                         employe.find(".//Telephone[@label='Mobile']").text,
                         employe.find(".//Telephone[@label='Other']").text
                         )
        data.append(tuple_data)

    return data
