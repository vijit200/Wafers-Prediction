from datetime import  datetime

class App_Logger:

    def __init__(self) -> None:
        pass

    # making a log for my model

    def log(self,file_name,message):


        self.now = datetime.now()
        # Taking day for my logger 
        self.date = self.now.date()
        #Taking time for my logger
        self.time = self.now.strftime("%H:%M:%S")

        file_name.write(str(self.date) + '/' + str(self.time) + ':' + ' '  + message + '\n')



