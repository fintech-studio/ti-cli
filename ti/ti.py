import argparse
from ti.services.config_service import ConfigService
from ti.services.database_service import DatabaseService
from ti.services.stock_data_service import StockDataService

# ANSI 顏色碼
class Colors:
    RESET = '\033[0m'
    BOLD = '\033[1m'
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    GRAY = '\033[90m'

def color_text(text, color):
    return f"{color}{text}{Colors.RESET}"

def main():
    parser = argparse.ArgumentParser(description="技術指標計算與交易訊號分析工具",add_help=False)

    # 建立子命令
    subparsers = parser.add_subparsers(dest='command', help='可用命令')

    #幫助訊息
    help_parser = subparsers.add_parser('help', help='顯示幫助訊息')

    # add 子命令 - 計算技術指標並分析交易訊號
    add_parser = subparsers.add_parser('add', help='計算技術指標並分析交易訊號')

    # 市場選項
    add_parser.add_argument('symbols', nargs='*', help='股票代碼列表 (例如: 2330 AAPL)')
    add_parser.add_argument('--tw', action='store_true', help='台股市場')
    add_parser.add_argument('--us', action='store_true', help='美股市場')
    add_parser.add_argument('--etf', action='store_true', help='ETF')
    add_parser.add_argument('--index', action='store_true', help='指數')
    add_parser.add_argument('--crypto', action='store_true', help='加密貨幣')
    add_parser.add_argument('--forex', action='store_true', help='外匯')
    add_parser.add_argument('--futures', action='store_true', help='期貨')

    # 時間選項
    add_parser.add_argument('--1m', action='store_true', help='1 分鐘數據')
    add_parser.add_argument('--5m', action='store_true', help='5 分鐘數據')
    add_parser.add_argument('--15m', action='store_true', help='15 分鐘數據')
    add_parser.add_argument('--30m', action='store_true', help='30 分鐘數據')
    add_parser.add_argument('--1h', action='store_true', help='1 小時數據')
    add_parser.add_argument('--1d', action='store_true', help='1 天數據')
    add_parser.add_argument('--1w', action='store_true', help='1 週數據')
    add_parser.add_argument('--1mo', action='store_true', help='1 月數據')

    # db 子命令 - 資料庫管理
    db_parser = subparsers.add_parser('db', help='資料庫配置與管理')
    db_parser.add_argument('--host', type=str, help='設定資料庫位址')
    db_parser.add_argument('--database', type=str, help='設定資料庫名稱')
    db_parser.add_argument('--user', type=str, help='設定資料庫使用者名稱')
    db_parser.add_argument('--password', type=str, help='設定資料庫使用者密碼')
    db_parser.add_argument('--driver', type=str, help='設定資料庫驅動程式名稱')
    db_parser.add_argument('--clear', action='store_true', help='清除資料庫設置')
    db_parser.add_argument('--config', action='store_true', help='顯示資料庫配置')
    db_parser.add_argument('--check', action='store_true', help='檢查資料庫連線')
    db_parser.add_argument('--tables',action='store_true',help='列出當前資料庫的資料表')

    args = parser.parse_args()

    if args.command == 'help' or args.command is None:
        show_help()
        return
    
    # 處理 add 子命令 - 計算技術指標並分析交易訊號
    if args.command == 'add':

        # 確定市場類型
        market = None
        if args.tw:
            market = 'tw'
        elif args.us:
            market = 'us'
        elif args.etf:
            market = 'etf'
        elif args.index:
            market = 'index'
        elif args.crypto:
            market = 'crypto'
        elif args.forex:
            market = 'forex'
        elif args.futures:
            market = 'futures'
        else:
            print("請指定市場類型 (例: --tw, --us, --crypto)")
            return
        
        # 確定時間選項
        time = None
        if args.__dict__.get('1m'):
            time = '1m'
        elif args.__dict__.get('5m'):
            time = '5m'
        elif args.__dict__.get('15m'):
            time = '15m'
        elif args.__dict__.get('30m'):
            time = '30m'
        elif args.__dict__.get('1h'):
            time = '1h'
        elif args.__dict__.get('1d'):
            time = '1d'
        elif args.__dict__.get('1w'):
            time = '1w'
        elif args.__dict__.get('1mo'):
            time = '1mo'
        else:
            print("請指定時間選項 (例: --1d, --1h)")
            return
        
        for symbol in args.symbols:
            print(f"處理股票代碼: {symbol} 市場: {market} 時間: {time}")
            # 在此處調用技術指標計算和交易訊號分析的相關函數
    
    # 處理 db 子命令 - 資料庫管理配置與管理
    if args.command == 'db':
        config_service = ConfigService()
        db_service = DatabaseService()
        
        has_args = any([args.clear, args.host, args.database, args.user, args.password, 
                       args.driver, args.config, args.check, args.tables])
        
        if args.clear:
            confirm = input("Confirm to clear all database settings? (y/n): ")
            if confirm.lower() == 'y':
                ClearMessage = config_service.clear_db_config()
                print(f"✓ {ClearMessage}")
            else:
                print("Operation cancelled")
            return
        
        if args.host or args.database or args.user or args.password or args.driver:
            DBUpdateMessage = config_service.update_db_config(
                server=args.host,
                database=args.database,
                username=args.user,
                password=args.password,
                driver=args.driver
            )
            config = config_service.show_db_config()
            for key, value in config.items():
                print(f"  {key}: {value}")

            print("\n")

            success, ifDBExitsMessage = db_service.create_database_if_not_exists(config.get('database'))
            print(f"  {ifDBExitsMessage}")
            print(f"✓ {DBUpdateMessage}")
        
        if args.config:
            config = config_service.show_db_config()
            for key, value in config.items():
                print(f"  {key}: {value}")
        
        if args.check:
            success, TestConnectMessage = db_service.test_connection()
            print(f"  {TestConnectMessage}")

        if args.tables:
            success, tables = db_service.list_tables()
            if success and tables:
                for i, table in enumerate(tables, 1):
                    print(f"  {i}. {table}")
            else:
                print("not available tables.")
        
        if not has_args:
            # 顯示配置資訊
            #print("\n")
            config = config_service.show_db_config()
            for key, value in config.items():
                print(f"  {key}: {value}")
            
            # 確保資料庫存在
            print("\n")
            success, ifDBExitsMessage = db_service.create_database_if_not_exists(config.get('database'))
            print(f"  {ifDBExitsMessage}")
            
            # 測試連線
            print("\n")
            success, TestConnectMessage = db_service.test_connection()
            print(f"  {TestConnectMessage}")
            
            # 列出資料表
            print("\n")
            success, tables = db_service.list_tables()
            if success and tables:
                for i, table in enumerate(tables, 1):
                    print(f"  {i}. {table}")
            else:
                print("not available tables.")
        
def show_help():
    help_text = f""""""