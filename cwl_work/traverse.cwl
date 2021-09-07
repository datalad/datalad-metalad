cwlVersion: v1.0
class: CommandLineTool
baseCommand: dataset_traverser.py
stdout: cwl.output.json
inputs:
  dataset:
    type: string
    inputBinding:
      position: 1
  recursive:
    type: boolean?
    inputBinding:
      position: 2
      prefix: -r

outputs:
  type: Any[]


