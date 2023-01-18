# Databricks notebook source
# import the time module
import time
from IPython.display import HTML, Markdown, display, display_markdown

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text(name="Time", defaultValue = "Hello")

# COMMAND ----------

def countdown(t):
    while t:
        mins, secs = divmod(t, 60)
        timer = '\x1b[1;33;46mBreak will end in: {:02d}:{:02d} \x1b[0m'.format(mins, secs)        
        print(timer, end="\r")
        time.sleep(15)
        t -= 1
        
t = 300
  
countdown(int(t))

# COMMAND ----------



# COMMAND ----------

v = dbutils.widgets.get("Time")
dd = display(HTML(f"<span STYLE='font-size:18.0pt'>Hello {v}</span>"),display_id=21)

# COMMAND ----------

dd = display(HTML("<span STYLE='font-size:18.0pt'>Hello</span>"),display_id=21)

def printmd(string):
  display(Markdown(string))
   
def print_html(text):
  dd.update(HTML(text))

def clear_html():
  dd.update(HTML("<span STYLE='font-size:18.0pt'></span>"))
  
def countdown1(t):
    while t:
        mins, secs = divmod(t, 60)
        #timer = '\x1b[1;33;46mBreak will end in: {:02d}:{:02d} \x1b[0m'.format(mins, secs)
        timer_html = "<span STYLE='font-size:18.0pt'>Break will end in: {:02d}:{:02d} </span>".format(mins, secs)
        #print(timer, end="\r")
        print_html(timer_html)
        time.sleep(1)
        clear_html()
        time.sleep(1)
        t -= 1
        
t = 300
  
countdown1(int(t))

# COMMAND ----------


dd = display(HTML("<span STYLE='font-size:18.0pt'>Hello</span>"),display_id=21)

time.sleep(5)

# COMMAND ----------

dd = display(display_id=11)
dd.update(HTML("<span STYLE='font-size:18.0pt'>Counter 1 </span>"))
dd.update(HTML("<span STYLE='font-size:18.0pt'>Counter 2 </span>"))

# COMMAND ----------

display("<span STYLE='font-size:18.0pt'>Counter 3 </span>",display_id=11).update(11)



# COMMAND ----------

?update_display

# COMMAND ----------

print('\x1b[170;0;0m'+'Hello world'+'\x1b[0m')


# COMMAND ----------

display().update("<span STYLE='font-size:18.0pt'>Counter 1 </span>")

display().update("<span STYLE='font-size:18.0pt'>Counter 2 </span>")

# COMMAND ----------


