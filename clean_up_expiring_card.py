import os
import logging
import requests
from datetime import datetime
from google.cloud import firestore
import functions_framework

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 配置
PROJECT_ID = os.environ.get('PROJECT_ID', 'seventh-program-433718-h8')
USER_BACKEND_URL = os.environ.get('USER_BACKEND_URL', 'https://user-backend-351785787544.us-central1.run.app')

# 初始化 Firestore
db = firestore.Client(project=PROJECT_ID)


def call_destroy_card_api(user_id, card_id, subcollection_name, quantity):
    """调用 Cloud Run destroy card API"""
    try:
        url = f"{USER_BACKEND_URL}/users/api/v1/users/{user_id}/cards/{card_id}"
        params = {
            'quantity': quantity,
            'subcollection_name': subcollection_name
        }

        logger.info(f"调用 API: {url} with params: {params}")

        response = requests.delete(url, params=params, timeout=30)

        if response.status_code == 200:
            logger.info(f"成功销毁卡片: {card_id} (用户: {user_id})")
            return True
        elif response.status_code == 404:
            logger.warning(f"卡片不存在: {card_id} (用户: {user_id})")
            return True  # 卡片不存在也算成功
        else:
            logger.error(f"API 失败: {response.status_code} - {response.text}")
            return False

    except Exception as e:
        logger.error(f"调用 API 出错: {str(e)}")
        return False


def cleanup_expired_cards():
    """扫描过期卡片并调用销毁 API"""
    logger.info("开始清理过期卡片")

    current_time = datetime.now()
    stats = {'scanned': 0, 'destroyed': 0, 'errors': 0}

    try:
        # 查询过期卡片
        expired_query = db.collection('expiring_cards').where('expiresAt', '<=', current_time)
        expired_cards = expired_query.stream()

        for doc in expired_cards:
            stats['scanned'] += 1
            data = doc.to_dict()

            # 解析数据
            user_id = data.get('userId')
            card_reference = data.get('cardReference')
            quantity = data.get('quantity', 1)

            if not user_id or not card_reference:
                logger.warning(f"无效数据: {data}")
                stats['errors'] += 1
                # 删除无效记录
                doc.reference.delete()
                continue

            # 解析卡片路径: users/{user_id}/cards/cards/{subcollection}/{card_id}
            try:
                path_parts = card_reference.split('/')
                if len(path_parts) != 6:
                    raise ValueError("路径格式错误")

                subcollection_name = path_parts[4]
                card_id = path_parts[5]

            except Exception as e:
                logger.error(f"解析路径失败 {card_reference}: {str(e)}")
                stats['errors'] += 1
                # 删除无效记录
                doc.reference.delete()
                continue

            logger.info(f"处理过期卡片: {user_id}/{card_id} (数量: {quantity})")

            # 调用销毁 API
            if call_destroy_card_api(user_id, card_id, subcollection_name, quantity):
                stats['destroyed'] += 1
                # 删除过期记录
                doc.reference.delete()
                logger.info(f"已删除过期记录: {doc.id}")
            else:
                stats['errors'] += 1

        logger.info(f"清理完成 - 扫描: {stats['scanned']}, 销毁: {stats['destroyed']}, 错误: {stats['errors']}")
        return stats

    except Exception as e:
        logger.error(f"清理过程出错: {str(e)}")
        raise


@functions_framework.cloud_event
def cleanup_expired_cards_scheduled(cloud_event):
    """定时触发入口"""
    logger.info("定时任务触发")
    try:
        result = cleanup_expired_cards()
        return {'status': 'success', 'stats': result}
    except Exception as e:
        logger.error(f"执行失败: {str(e)}")
        return {'status': 'error', 'message': str(e)}


@functions_framework.http
def cleanup_expired_cards_http(request):
    """HTTP 触发入口（测试用）"""
    logger.info("HTTP 请求触发")
    try:
        result = cleanup_expired_cards()
        return {
            'success': True,
            'message': '清理完成',
            'stats': result
        }
    except Exception as e:
        logger.error(f"执行失败: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }, 500


# 本地测试
if __name__ == '__main__':
    print("开始本地测试...")
    try:
        result = cleanup_expired_cards()
        print(f"测试完成: {result}")
    except Exception as e:
        print(f"测试失败: {str(e)}")
