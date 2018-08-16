from SemSL._slConfigManager import slConfig

def test_slConfig():
    sl_config = slConfig()
    print (sl_config["hosts"]["minio"]["alias"])

if __name__ == "__main__":
    test_slConfig()
