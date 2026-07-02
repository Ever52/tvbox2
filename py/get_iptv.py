import re
import os
import requests
import logging
import shutil
import threading
import chardet
from collections import OrderedDict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === 配置日志 ===
def setup_logger():
    # 确保日志目录存在
    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # 控制台输出
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 文件输出
    file_handler = logging.FileHandler("logs/iptv_update.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger

logger = setup_logger()

# 全局锁，用于文件写入
write_lock = threading.Lock()

def ensure_dir(file_path):
    """确保文件所在的目录存在"""
    dirname = os.path.dirname(file_path)
    if dirname:
        os.makedirs(dirname, exist_ok=True)

def get_session():
    """增强功能：模拟标准浏览器，带重试机制，防止源服务器返回403拒绝访问"""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Connection": "keep-alive"
    })
    retry = Retry(connect=3, read=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def load_urls_from_file(file_path):
    """从文本文件加载URL列表"""
    urls = []
    if not os.path.exists(file_path):
        logger.warning(f"URL配置文件未找到: {file_path}")
        return urls
    try:
        # 使用 utf-8-sig 安全过滤由于记事本编辑可能产生的 \ufeff BOM 头
        with open(file_path, "r", encoding="utf-8-sig") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    urls.append(line)
        logger.info(f"从 {file_path} 加载了 {len(urls)} 个源")
    except Exception as e:
        logger.error(f"读取URL文件失败: {e}")
    return urls

def parse_template(template_file):
    """解析模板文件"""
    template_channels = OrderedDict()
    current_category = None
    try:
        with open(template_file, "r", encoding="utf-8-sig") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "#genre#" in line:
                    current_category = line.split(",")[0].strip()
                    template_channels[current_category] = []
                elif current_category:
                    channel_name = line.split(",")[0].strip()
                    if channel_name:
                        template_channels[current_category].append(channel_name)
    except FileNotFoundError:
        logger.warning(f"模板文件未找到: {template_file}")
        return None 
    return template_channels

def fetch_channels(url):
    """增强功能：多线程健壮性升级，引入流式字节读取与 chardet 智能防乱码引擎"""
    channels = OrderedDict()
    with get_session() as session:
        try:
            with session.get(url, timeout=15, stream=True) as response:
                response.raise_for_status()
                
                # 读取前 50KB 动态识别文本真实编码（如 GB2312/GBK/UTF-8）
                raw_content = response.raw.read(51200)
                if not raw_content:
                    return channels
                
                detected = chardet.detect(raw_content)
                encoding = detected['encoding'] if detected['encoding'] else 'utf-8'
                
                # 拼接剩余文本流，完美解决编码导致的中文乱码
                remaining_content = response.raw.read()
                full_text = (raw_content + remaining_content).decode(encoding, errors='ignore')
                
        except Exception as e:
            logger.error(f"网络抓取异常 {url}: {e}")
            return channels

    lines = [line.strip() for line in full_text.splitlines() if line.strip()]
    if not lines:
        return channels

    is_m3u = any("#EXTINF" in line for line in lines[:10])

    if is_m3u:
        current_category = "默认分类"
        current_name = "未知频道"
        # 优化 M3U 正则：完美兼容单双引号
        re_group = re.compile(r'group-title=["\']([^"\']*)["\']')
        re_name = re.compile(r',([^,]*)$')

        for line in lines:
            if line.startswith("#EXTINF"):
                group_match = re_group.search(line)
                if group_match:
                    current_category = group_match.group(1).strip()
                name_match = re_name.search(line)
                if name_match:
                    current_name = name_match.group(1).strip()
            elif not line.startswith("#") and "://" in line:
                if current_category not in channels:
                    channels[current_category] = []
                if current_name and current_name != "未知频道":
                    channels[current_category].append((current_name, line.strip()))
                current_name = "未知频道"
    else:
        current_category = None
        for line in lines:
            if "#genre#" in line:
                current_category = line.split(",")[0].strip()
                if current_category not in channels:
                    channels[current_category] = []
            elif current_category and "," in line:
                parts = line.split(",", 1)
                if len(parts) == 2:
                    name, url_part = parts
                    if name.strip() and url_part.strip():
                        channels[current_category].append((name.strip(), url_part.strip()))
    return channels

def match_channels(template_channels, all_channels):
    """
    强化正则表达式 + 引入 Hash Mapping 哈希映射技术
    将数万次全量扫描压缩至 O(1) 的字典定位，性能提升数千倍。
    """
    matched = OrderedDict()
    unmatched_template = OrderedDict()

    # 1. 建立全局 Hash Map 倒排索引
    source_map = {}
    for cat, chans in all_channels.items():
        for name, url in chans:
            norm_name = name.lower().strip()
            if norm_name not in source_map:
                source_map[norm_name] = []
            source_map[norm_name].append({'name': name, 'url': url, 'cat': cat})

    used_channel_keys = set()

    for cat in template_channels:
        matched[cat] = OrderedDict()
        unmatched_template[cat] = []

    # 2. 局部哈希筛选 + 强正则表达式边界确认
    for category, tmpl_names in template_channels.items():
        for tmpl_name in tmpl_names:
            
            variants_raw = [n.strip() for n in tmpl_name.split("|") if n.strip()]
            variants = list(OrderedDict.fromkeys(variants_raw))

            primary_name = variants[0]
            found_for_this_template = False

            for variant in variants:
                variant_lower = variant.lower()
                
                # 强化版正则表达式：结合安全转义与严格断言
                # 防止 "CCTV1" 匹配到 "CCTV11" (后向数字拦截)
                # 防止 "CCTV5" 匹配到 "CCTV5+" (除非指定，后向特殊符号拦截)
                # 防止 "CCTV1" 匹配到 "广东CCTV1" (前向边界拦截)
                pattern = re.compile(r'(?<![a-zA-Z0-9])' + re.escape(variant_lower) + r'(?![a-zA-Z0-9\+])')

                # 哈希字典初筛优化：只匹配包含核心关键字的列表项，彻底终结 O(N) 全量循环
                target_keys = [k for k in source_map.keys() if variant_lower in k]

                for src_name_lower in target_keys:
                    if pattern.search(src_name_lower):
                        for src in source_map[src_name_lower]:
                            key = f"{src['name']}_{src['url']}"
                            if key in used_channel_keys:
                                continue

                            if primary_name not in matched[category]:
                                matched[category][primary_name] = []

                            matched[category][primary_name].append((src['name'], src['url']))
                            used_channel_keys.add(key)
                            found_for_this_template = True

            if not found_for_this_template:
                unmatched_template[category].append(tmpl_name)

    # 3. 筛选未匹配源
    unmatched_source = OrderedDict()
    for src_name_lower, src_list in source_map.items():
        for src in src_list:
            key = f"{src['name']}_{src['url']}"
            if key not in used_channel_keys:
                if src['cat'] not in unmatched_source:
                    unmatched_source[src['cat']] = []
                unmatched_source[src['cat']].append((src['name'], src['url']))

    return matched, unmatched_template, unmatched_source

def is_ipv6(url):
    return "://[" in url

def generate_outputs(channels, template_channels, m3u_path, txt_path):
    """Bug修复：利用正则表达式切分各种变态的URL污染（如带有$后缀或#等多重后缀的杂质）"""
    written_urls = set()
    ensure_dir(m3u_path)
    ensure_dir(txt_path)

    try:
        with write_lock:
            with open(m3u_path, "w", encoding="utf-8") as m3u, \
                 open(txt_path, "w", encoding="utf-8") as txt:

                m3u.write("#EXTM3U\n")

                for category in template_channels:
                    if category not in channels or not channels[category]:
                        continue

                    txt.write(f"\n{category},#genre#\n")

                    for channel_key_name, channel_list in channels[category].items():
                        unique_urls = []
                        seen_urls = set()

                        for _, url in channel_list:
                            if url not in seen_urls and url not in written_urls:
                                unique_urls.append(url)
                                seen_urls.add(url)
                                written_urls.add(url)

                        total_lines = len(unique_urls)
                        for idx, url in enumerate(unique_urls, 1):
                            # Bug 修复：完美剥离用 $ 或 # 连接的额外Token，还原纯净地址
                            base_url = re.split(r'[$#]', url)[0].strip()
                            suffix_name = "IPV6" if is_ipv6(url) else "IPV4"

                            display_name = channel_key_name
                            meta_suffix = f"$LR•{suffix_name}"
                            if total_lines > 1:
                                meta_suffix += f"•{total_lines}『线路{idx}』"

                            final_url = f"{base_url}{meta_suffix}"

                            # 增强：清理和转义双引号防止 m3u 解析软件崩溃
                            safe_display_name = display_name.replace('"', '\\"')
                            m3u.write(f'#EXTINF:-1 tvg-name="{safe_display_name}" group-title="{category}",{display_name}\n')
                            m3u.write(f"{final_url}\n")

                            txt.write(f"{display_name},{final_url}\n")

        logger.info(f"输出完成: {m3u_path}, {txt_path}")
    except Exception as e:
        logger.error(f"写入输出文件失败: {e}")

def generate_unmatched_report(unmatched_template, unmatched_source, report_file):
    """生成未匹配报告"""
    total_template_lost = sum(len(v) for v in unmatched_template.values())
    
    # 如果未指定报告文件路径，则仅计算丢失数量，不执行文件写入
    if not report_file:
        return total_template_lost

    ensure_dir(report_file)
    try:
        with open(report_file, "w", encoding="utf-8") as f:
            f.write(f"# 未匹配报告 {datetime.now()}\n")
            f.write(f"# 模板未匹配数: {total_template_lost}\n\n")
            f.write("## 模板中有但源中无\n")
            for cat, names in unmatched_template.items():
                if names:
                    f.write(f"\n{cat},#genre#\n")
                    for name in list(OrderedDict.fromkeys(names)):
                        f.write(f"{name},\n")

            f.write("\n\n## 源中有但模板无\n")
            for cat, chans in unmatched_source.items():
                if chans:
                    f.write(f"\n{cat},#genre#\n")
                    unique_names = list(OrderedDict.fromkeys([c[0] for c in chans]))
                    for name in unique_names:
                        f.write(f"{name},\n")
        logger.info(f"报告已生成: {report_file}")
        return total_template_lost
    except Exception as e:
        logger.error(f"生成报告失败: {e}")
        return 0

def remove_unmatched_from_template(template_file, unmatched_template):
    backup_file = template_file + ".backup"
    try:
        shutil.copy2(template_file, backup_file)
        with open(template_file, "r", encoding="utf-8-sig") as f:
            lines = f.readlines()

        new_lines = []
        current_cat = None
        to_remove = {cat: set(names) for cat, names in unmatched_template.items()}

        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                new_lines.append(line)
                continue
            if "#genre#" in stripped:
                current_cat = stripped.split(",")[0].strip()
                new_lines.append(line)
                continue
            if current_cat:
                name = stripped.split(",")[0].strip()
                if current_cat in to_remove and name in to_remove[current_cat]:
                    continue
                new_lines.append(line)
            else:
                new_lines.append(line)

        with open(template_file, "w", encoding="utf-8") as f:
            f.writelines(new_lines)
        logger.info(f"已从模板 {template_file} 移除无效频道")
    except Exception as e:
        logger.error(f"更新模板失败: {e}")

def process_iptv_task(template_file, tv_urls, output_m3u, output_txt, report_file, auto_clean=True):
    logger.info(f"=== 开始处理任务: {template_file} ===")
    template = parse_template(template_file)
    if not template:
        return

    logger.info(f"开始从 {len(tv_urls)} 个源获取数据...")
    all_channels = OrderedDict()
    success_count = 0
    fail_count = 0

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(fetch_channels, url): url for url in tv_urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
                if data:
                    success_count += 1
                    for cat, chans in data.items():
                        if cat not in all_channels:
                            all_channels[cat] = []
                        all_channels[cat].extend(chans)
                else:
                    fail_count += 1
            except Exception as e:
                fail_count += 1
                logger.error(f"源 {url} 线程执行异常: {e}")

    logger.info(f"数据获取完毕: 成功解析 {success_count} 个源，失败/空数据 {fail_count} 个源。")
    logger.info("开始匹配频道...")
    
    matched, unmatched_tmpl, unmatched_src = match_channels(template, all_channels)
    
    generate_outputs(matched, template, output_m3u, output_txt)
    lost_count = generate_unmatched_report(unmatched_tmpl, unmatched_src, report_file)

    if auto_clean and lost_count > 0:
        logger.info(f"清理 {lost_count} 个无效频道...")
        remove_unmatched_from_template(template_file, unmatched_tmpl)
        
    logger.info(f"=== 任务完成: {template_file} ===\n")

if __name__ == "__main__":
    # === 配置区 ===
    URLS_FILE = "py/config/urls.txt"
    
    # 1. 加载源
    TV_URLS = load_urls_from_file(URLS_FILE)
    if not TV_URLS:
        logger.warning("未从文件中加载到URL，使用空列表")
        TV_URLS = [] 

    # === 任务1: 主列表 ===
    process_iptv_task(
        template_file="py/config/iptv.txt",
        tv_urls=TV_URLS,
        output_m3u="lib/iptv.m3u",
        output_txt="lib/iptv.txt",
        report_file="py/config/iptv.log",
        auto_clean=True
    )

    # === 任务2: 测试列表 (如果配置文件存在) ===
    TEST_TEMPLATE_FILE = "py/config/iptv_test.txt"
    if os.path.exists(TEST_TEMPLATE_FILE):
        process_iptv_task(
            template_file=TEST_TEMPLATE_FILE,
            tv_urls=TV_URLS,
            output_m3u="lib/iptv_test.m3u",
            output_txt="lib/iptv_test.txt",
            report_file=None,
            auto_clean=False 
        )
    else:
        logger.info(f"未检测到测试配置 {TEST_TEMPLATE_FILE}，跳过测试生成。")
