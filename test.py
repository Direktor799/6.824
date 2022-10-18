import os
from sys import argv


class Tester:
    def __init__(self, lab: str):
        self.lab = lab

    def run(self, times: int):
        os.system("rm -rf {}-logs".format(self.lab))
        os.makedirs("{}-logs".format(self.lab))
        for i in range(times):
            os.system(
                "cd ./src/raft; go test -race -run {0} > ../../{0}-logs/{1}.log".format(self.lab, i))


REPEAT_TIMES = 1000

if __name__ == '__main__':
    if len(argv) < 2 or len(argv) > 3:
        print("Usage: python3 test.py [lab_num] [times (default 1000)]")
        quit()
    try:
        REPEAT_TIMES = int(argv[2])
    except:
        print("Can not parse times arg '{}' into number".format(argv[2]))
        quit()

    tester = Tester(argv[1])
    tester.run(REPEAT_TIMES)
