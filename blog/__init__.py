from .views import app
from .models import User

user = User("")
user.create_user_constraint()
user.create_post_constraint()
# user.create_tag_constraint()
user.create_post_index()


