import subprocess

if __name__ == '__main__':
   
    process1 = subprocess.Popen(['python', 'main.py'])
   
    process2 = subprocess.Popen(['python', 'gui.py'])

    process1.wait()
    process2.wait()

    print("Both processes completed.")