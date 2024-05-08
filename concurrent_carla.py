import subprocess

if __name__ == '__main__':
   
    process1 = subprocess.Popen(['python', 'carla_1.py'])
   
    process2 = subprocess.Popen(['python', 'carla_2.py'])

    process1.wait()
    process2.wait()

    print("Both processes completed.")