class Volume:
    def __init__(self,name,size,type):
     self.name = name
     self.size = size
     self.type = type

    def snapshot(self,type):
        print(f"snapshot policy is enabled for volume:{type}")
              
Vol = Volume("test",10,"NAS")
print(Vol.name)
print(Vol.size)
print(Vol.type)

Vol.snapshot("24-hours")
