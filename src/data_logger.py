#!/usr/bin/env python
import sys
import rospy
import rosgraph
import time
from roslib.message import get_message_class
from std_msgs.msg import *
from edg_data_logger.srv import *
from rospy.msg import AnyMsg


#Current state of logging
isLoggingEnabled = False

#List of topics to record
listOfTopics = []

#List of Subscribers so we can easily Unsubscribe to topics.
listOfSubscribers = []

#List of available topics and their corresponding types
topic_types = []

#Data record before it gets dumped in the CSV file
record = {}

#Output CSV file used, a new file is generated every time the user starts a
#new data logging process.
output_file_name = ''

#Wait until we get at least one value from each topic and then
#append the CSV file with the informations.
def appendDataPoint(topic, value):
    global record
    global listOfTopics
    global output_file

    record.update({topic: value})
    #If we got one value for each topic, we append the CSV file.
    if len(record) == len(listOfTopics) and len(record) > 0:
        #The timestamp is the time since the 1st of January 1970 (unix timestamp)
        line = str(time.time())+','
        #For each topic, append its value
        for topic in listOfTopics:
            line = line + str(record[topic]) + ','
        line = line[0:-1] + '\n'
        output_file.write(line)
        record = {}

#This callback function is triggered every time a message was sent to a
#subscribed topic.
def callback(data, args):
    #Assume that the first argument represents the name of the type of the message
    topic = args[0]
    msg_name = args[1]

    #Retrieve the class associated with that message name
    msg_class = get_message_class(msg_name)
    #Transform the message data into an instance of the message class
    msg = msg_class().deserialize(data._buff)
    value = msg.data
    appendDataPoint(topic, value)

#Iterates over the list of subscribers and unregister them in order to stop
#the callback functions from being triggered.
def unsubscribeAllTopics():
    global listOfTopics
    global listOfSubscribers

    for sub in listOfSubscribers:
        sub.unregister()

    listOfTopics = list()
    listOfSubscribers = list()

#Reads a list of ROS topics from the configuration file into a global list
def loadConfigFile(filePath):
    global listOfTopics
    global listOfSubscribers
    global topic_types
    #Reset the subscription
    unsubscribeAllTopics()

    #Iterates over every line of the configuration file and subscribe to each
    #topic.
    with open(filePath) as fp:
        for line in fp:
            #The line contains one or more newline characters that needs to be removed
            topic = line.replace('\n','').replace('\r','').replace(' ','')
            #Retrieve the message type associated with that topic
            msg_name = ""
            for couple in topic_types:
                tp = couple[0]
                ty = couple[1]
                if tp == topic:
                    msg_name = ty
            #As not every topic uses the same type of message, AnyMsg is used.
            #http://docs.ros.org/melodic/api/rospy/html/rospy.msg.AnyMsg-class.html
            #The message name is passed as an argument so the callback function can use it
            sub = rospy.Subscriber(topic, AnyMsg, callback, (topic,msg_name))
            #It is very important that the n-th subscriber correponds to the n-th topic
            #in these lists as its assumed afterward.
            listOfTopics.append(topic)
            listOfSubscribers.append(sub)


#This callback function sets the state of the logging process (Enabled or Disable)
#when the user calls this service.
def setLoggingState(request):
    global isLoggingEnabled
    global output_file
    global output_file_name
    global listOfTopics
    global record
    #This field of the request object contains the value set by the user when
    #calling this service.
    desiredState = request.EnableDataLogging

    #Enable logging
    if desiredState == True:
        #If its not already in that state
        if isLoggingEnabled != desiredState:
            #Loads the edg_data_logger/config/TopicsList.txt file
            loadConfigFile('/home/edg/catkin_ws/src/edg_data_logger/config/TopicsList.txt')
            print("Listening for these topics: "+str(listOfTopics))
            #Generate a new for the output CSV file
            output_file_name = '/tmp/data_log_'+str(int(time.time()))+'.csv'
            #Open the CSV file for writing
            output_file = open(output_file_name,'w')
            #Write the header of the CSV file with all the topic names
            header = 'timestamp,'
            for topic in listOfTopics:
                header = header + topic + ','
            header = header[0:-1] + '\n'
            output_file.write(header)
            print("Writing output to: "+output_file_name)
        print("Data logging is started.")

    #Disable logging
    if desiredState == False:
        #If its not already in that state
        if isLoggingEnabled != desiredState:
            unsubscribeAllTopics()
            output_file.close()
            record = {}
        print("Data logging is stopped.")

    isLoggingEnabled = desiredState

    #This is what gets returned to the user
    return EnableResponse(output_file_name)


if __name__ == '__main__':

    #Retrieve the graph of nodes
    master = rosgraph.Master(rospy.get_name())

    #Get a list of all topics and their associated message type
    topic_types = master.getTopicTypes()

    rospy.init_node("edg_log_node", anonymous=True)

    #Advertise our service
    service = rospy.Service('data_logging', Enable, setLoggingState)



    rospy.spin()
