#!/usr/bin/env python
from rich.console import Console
from rich.columns import Columns
from sys import stdin
from plac import pos,flg,opt,call
from typing import List

D_TOPICS = {
    "TIMR": "bright_black",
    "VOTE": "bright_cyan",
    "LEAD": "yellow",
    "TERM": "green",
    "LOG1": "blue",
    "LOG2": "cyan",
    "CMIT": "magenta",
    "PERS": "white",
    "SNAP": "bright_blue",
    "DROP": "bright_red",
    "CLNT": "bright_green",
    "TEST": "bright_magenta",
    "INFO": "bright_white",
    "WARN": "bright_yellow",
    "ERRO": "red",
    "TRCE": "red",
}

class InvalidInputExpection(Exception):
    def __init__(self, *args: object) -> None:
        msg = args[0]
        super().__init__(*args[1:])
        self.msg = msg

    def __str__(self) -> str:
        return self.msg


console = Console(color_system="auto",)
width = console.size.width
height = console.size.height
is_terminal = console.is_terminal

def validate_list(provided_input:List) -> List[str]:
    for input_ in provided_input:
        if input_ not in set(D_TOPICS):
            raise InvalidInputExpection(f"a/an invalid input was provided in topic")
    return provided_input

@opt("file_path","file path to use as input ",type=str)
@opt("ignore_topic","debug levels to ignore ",type=List[str])
@flg("colorize","include colors")
@pos("n_columns","number of column in the console",type=int)
def main(n_columns:int,file_path:str="",ignore_topic:List[str] =[],colorize:bool=False):
    ignore_topic = validate_list(ignore_topic)
    
    if not is_terminal:
        return
     
    if n_columns < 1 :
        console.print("[red]n_columns cannot be less than 1[/red]")
        return

    ignore_topic = list(set(ignore_topic))
    topics = list(set(D_TOPICS.keys()))

    contents = [] #buffer content into this place

    if len(file_path):
        with open(file_path,"r") as f:
            contents = f.readlines()
            f.close()
    else:
        contents = stdin.readlines()

    if len(ignore_topic):
        topics =  [topic  for topic in topics if  topic not in ignore_topic ] 

    panic = False

    with console.pager(styles=True):
        for line in contents:
            try: 
                time = int(line[:6])
                topic = line[7:11]
                msg = line[12:].strip()

                if topic not in topics: # skip line that we don't need
                    continue
                
                if topic != "TEST" and n_columns:
                    i = int(msg[1])
                    msg = msg[3:]

                if colorize and topic in topics: # to add color
                    color = D_TOPICS[topic]
                    msg =  f'[{color}]{msg}[/{color}]'

                if n_columns is None or topic == "TEST":
                    console.print(time, msg)
                else:
                    cols = ["" for _ in range(n_columns)]
                    cols[i] = msg
                    cols_width =  int(width/n_columns)
                    cols =  Columns(cols,width=cols_width - 1,expand=True,equal=True)
                    console.print(cols)
            except:
                if line.startswith("panic"):
                    panic = True
                if not panic:
                    console.print("[y ellow][b]#" * console.width,)
                console.print(line, end="")


                
if __name__ == "__main__":
    call(main)





