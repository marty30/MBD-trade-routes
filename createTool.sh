#!/usr/bin/env python
import os
import sys
import string

def createFromScalaTemplate(toolName):
  if toolName.endswith(".scala"):
    toolName = toolName[:-6]
  src_fn = "templates/scala/Template.scala"
  dest_dir = "src/main/scala/nl/utwente/bigdata"
  env = dict(os.environ)
  env['toolName'] = toolName
  dst_fn = os.path.join(dest_dir, toolName + '.scala')
  with open(src_fn) as f, open(dst_fn, 'w') as out:
      t = string.Template(f.read())
      out.write(t.safe_substitute(env))  

def createFromPythonTemplate(toolName):
  if toolName.endswith(".py"):
    toolName = toolName[:-3]
  src_fn = "templates/python/Template.py"
  dest_dir = "src/main/python"
  if not os.path.isdir(dest_dir):
    os.mkdir(dest_dir)
  env = dict(os.environ)
  env['toolName'] = toolName
  dst_fn = os.path.join(dest_dir, toolName + '.py')
  with open(src_fn) as f, open(dst_fn, 'w') as out:
      t = string.Template(f.read())
      out.write(t.safe_substitute(env))  

def main():
  iarg = 1
  if sys.argv[1] in ['python', 'scala']:
    toolType = sys.argv[iarg]
    iarg += 1
  else:
    toolType = 'scala'
  
  toolName = sys.argv[iarg]
  
  if toolType == 'scala':
    createFromScalaTemplate(toolName)
  elif toolType == 'python':
    createFromPythonTemplate(toolName)

if __name__ == '__main__':
  main()