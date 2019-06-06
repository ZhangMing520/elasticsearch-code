1. 生成JSON文档
- 使用byte[]或者string字符串
- 使用map类型
- 使用第三方库序列化实体（Jackson）
- 使用内建的XContentFactory.jsonBuilder()

> 在内部处理过程，本质都是转化为byte[]。内置的jsonBuilder是一个高性能的JSON生成器，直接生成byte[]



```
# 禁用自动创建index
# "action.auto_create_index": "twitter,index10,-index1*,+ind*"  支持只在
PUT _cluster/settings
{
    "persistent": {
        "action.auto_create_index": "false" 
         
    }
}
```