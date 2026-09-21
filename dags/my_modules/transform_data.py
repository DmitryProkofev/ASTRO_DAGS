def parse(data: bytes) -> list[tuple]:
    """Парсит XML-байты и возвращает список кортежей."""
    from lxml import etree

    root = etree.fromstring(data)
    data_list = root.xpath("//DirectoryEntry") 
    result = []  
    
    for employe in data_list:
        def get_text(xpath_expr):
            element = employe.find(xpath_expr)
            if element is not None and element.text is not None:
                return element.text.strip()
            return None

        tuple_data = (
            get_text(".//Name"),
            get_text(".//Telephone[@label='Email']"),
            get_text(".//Telephone[@label='Work']"),
            get_text(".//Telephone[@label='Mobile']"),
            get_text(".//Telephone[@label='Company']")
        )
        result.append(tuple_data)

    return result