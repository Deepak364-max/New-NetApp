from dataclasses import dataclass 
@dataclass
class Volume:
    name:str
    size:int
    type:str

Vol = Volume("Test1",5, "SAN")
print(Vol.name)
print(Vol.size)
print(Vol.type)