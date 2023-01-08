from pydantic import BaseModel, Field


class PricePaidDefinition(BaseModel):
    id: str = Field(
        ...,
        description="""A reference number which is generated automatically recording each published sale. 
        The number is unique and will change each time a sale is recorded.""",
    )
    name: str = Field(..., max_length=20)
    height: float = Field(..., ge=0, le=250, description="Height in cm.", chris=True)


print(PricePaidDefinition.schema_json(indent=2))

#     - id
#     - price
#     - date
#     - postcode
#     - type
#     - new_build
#     - land
#     - primary_address
#     - secondary_address
#     - street
#     - locality
#     - town_city
#     - district
#     - county
#     - ppd
#     - record


# @validate_data_schema(data_schema=AvatarFrameDefinition)
# def return_user_avatars(user_id: int) -> pd.DataFrame:
#     # Let's use the user_id as the height for the Mustermann avatar to trigger the validation.
#     return pd.DataFrame(
#          [
#          {"id": user_id, "name": "Sebastian", "height": 178.0},
#          {"id": user_id, "name": "Max", "height": 218.0},
#          {"id": user_id, "name": "Mustermann", "height": user_id },
#         ]
#     )

# data = return_user_avatars(user_id=42)  # works

# # data_types = AvatarFrameDefinition(id=42, name="Chris", height=20.0).height.__annotations__

# # print(AvatarFrameDefinition.schema_json(indent=4))

# print(type(AvatarFrameDefinition.schema_json(indent=2)))

# print(AvatarFrameDefinition.schema_json(indent=2))

# res = json.loads(AvatarFrameDefinition.schema_json(indent=2))


# print(res["properties"])

# print(type(res))
