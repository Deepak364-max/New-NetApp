import argparse

parser = argparse.ArgumentParser(
    description="testing args_parse functionality"
)
parser.add_argument("Num1", help = "first number")
parser.add_argument("Num2", help = "Second number")
args = parser.parse_args()
print(args.Num1)
print(args.Num2)