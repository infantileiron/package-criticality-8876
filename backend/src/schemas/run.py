from pydantic import BaseModel,ConfigDict
class RunInput(BaseModel):
 model_config=ConfigDict(extra='forbid',allow_inf_nan=False)
 scenario_id:str
