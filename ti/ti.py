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

    # add 子命令 - 計算技術指標並分檢測k線型態
    add_parser = subparsers.add_parser('add', help='計算技術指標並分檢測k線型態')

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
    
    # 處理 add 子命令 - 計算技術指標並分析檢測k線型態
    if args.command == 'add':
        service = StockDataService()
        
        if not args.symbols:
            print("請提供至少一個股票代號")
            print("範例: ti add 2330 --tw --1d")
            print("      ti add AAPL --us --1h")
            return

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
            time = '1wk'
        elif args.__dict__.get('1mo'):
            time = '1mo'
        else:
            print("請指定時間選項 (例: --1d, --1h)")
            return
        
        for symbol in args.symbols:
            try:
                print(f"正在處理 {symbol} ({market}, {time})...")
                result = service.fetch_and_store(symbol, market, time)
                print(f"✓ {symbol} 技術指標資料已成功儲存")
                print(f"  - 獲取了 {result['data_count']} 筆股票數據")
                print(f"  - 計算了 {result['indicator_count']} 個技術指標")
                print(f"  - 檢測了 {result['pattern_count']} 筆K線型態資料")
                print(f"  - 數據已保存至資料表 stock_data_{time}")
                
            except Exception as e:
                print(f"✗ {symbol} 處理失敗: {str(e)}")
    
    # 處理 db 子命令 - 資料庫管理配置與管理
    if args.command == 'db':
        config_service = ConfigService()
        db_service = DatabaseService()
        
        has_args = any([args.clear, args.host, args.database, args.user, args.password, 
                       args.driver, args.config, args.check, args.tables])
        
        if args.clear:
            confirm = input("Confirm to clear all database settings? (y/n): ")
            if confirm.lower() == 'y':
                clear_message = config_service.clear_db_config()
                print(f"✓ {clear_message}")
            else:
                print("Operation cancelled")
            return
        
        if args.host or args.database or args.user or args.password or args.driver:
            db_update_message = config_service.update_db_config(
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

            success, if_db_exits_message = db_service.create_database_if_not_exists(config.get('database'))
            print(f"  {if_db_exits_message}")
            print(f"✓ {db_update_message}")
        
        if args.config:
            config = config_service.show_db_config()
            for key, value in config.items():
                print(f"  {key}: {value}")
        
        if args.check:
            success, test_connect_message = db_service.test_connection()
            print(f"  {test_connect_message}")

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
            success, if_db_exits_message = db_service.create_database_if_not_exists(config.get('database'))
            print(f"  {if_db_exits_message}")
            
            # 測試連線
            print("\n")
            success, test_connect_message = db_service.test_connection()
            print(f"  {test_connect_message}")
            
            # 列出資料表
            print("\n")
            success, tables = db_service.list_tables()
            if success and tables:
                for i, table in enumerate(tables, 1):
                    print(f"  {i}. {table}")
            else:
                print("not available tables.")
        
def show_help():
    help_text = f"""
{color_text('Technical Indicators Analysis System', Colors.BOLD + Colors.CYAN)}

{color_text('Basic Usage:', Colors.BOLD + Colors.YELLOW)}
  {color_text('ti', Colors.GREEN)} {color_text('[command]', Colors.BLUE)} {color_text('[options]', Colors.MAGENTA)}

{color_text('Subcommands:', Colors.BOLD + Colors.YELLOW)}
  {color_text('ti add', Colors.GREEN)}                               Calculate technical indicators and analyze trading signals
  {color_text('ti db', Colors.GREEN)}                                Database configuration and management

{color_text('Technical Analysis:', Colors.BOLD + Colors.YELLOW)}
  {color_text('ti add', Colors.GREEN)} {color_text('<stock_symbol>', Colors.BLUE)} {color_text('--<market>', Colors.MAGENTA)} {color_text('--<interval>', Colors.MAGENTA)}   Analyze stock with technical indicators

{color_text('Market Options:', Colors.BOLD + Colors.YELLOW)}
  {color_text('--tw', Colors.MAGENTA)}          Taiwan Stock Exchange
  {color_text('--us', Colors.MAGENTA)}          US Stock Market
  {color_text('--etf', Colors.MAGENTA)}         ETF
  {color_text('--index', Colors.MAGENTA)}       Index
  {color_text('--crypto', Colors.MAGENTA)}      Cryptocurrency
  {color_text('--forex', Colors.MAGENTA)}       Foreign Exchange
  {color_text('--futures', Colors.MAGENTA)}     Futures

{color_text('Time Intervals:', Colors.BOLD + Colors.YELLOW)}
  {color_text('--1m', Colors.MAGENTA)}          1 minute data
  {color_text('--5m', Colors.MAGENTA)}          5 minutes data
  {color_text('--15m', Colors.MAGENTA)}         15 minutes data
  {color_text('--30m', Colors.MAGENTA)}         30 minutes data
  {color_text('--1h', Colors.MAGENTA)}          1 hour data
  {color_text('--1d', Colors.MAGENTA)}          1 day data
  {color_text('--1wk', Colors.MAGENTA)}         1 week data
  {color_text('--1mo', Colors.MAGENTA)}         1 month data

{color_text('Database Configuration:', Colors.BOLD + Colors.YELLOW)}
  {color_text('ti db --host', Colors.GREEN)} {color_text('<address>', Colors.BLUE)}               Set database host
  {color_text('ti db --database', Colors.GREEN)} {color_text('<name>', Colors.BLUE)}              Set database name
  {color_text('ti db --user', Colors.GREEN)} {color_text('<username>', Colors.BLUE)}              Set database username
  {color_text('ti db --password', Colors.GREEN)} {color_text('<password>', Colors.BLUE)}          Set database password
  {color_text('ti db --driver', Colors.GREEN)} {color_text('<driver>', Colors.BLUE)}              Set database driver
  {color_text('ti db --clear', Colors.GREEN)}                        Clear all database settings
  {color_text('ti db --config', Colors.GREEN)}                       Show database configuration
  {color_text('ti db --check', Colors.GREEN)}                        Check database connection
  {color_text('ti db --tables', Colors.GREEN)}                       Show database tables

{color_text('Usage Examples:', Colors.BOLD + Colors.YELLOW)}
  {color_text('# Configure database', Colors.GRAY)}
  {color_text('ti db --host localhost --database TiDB --user sa --password YourPassword', Colors.GREEN)}
  
  {color_text('# Analyze Taiwan stocks', Colors.GRAY)}
  {color_text('ti add 2330 --tw --1d', Colors.GREEN)}
  {color_text('ti add 0050 --tw --1h', Colors.GREEN)}
  
  {color_text('# Analyze US stocks', Colors.GRAY)}
  {color_text('ti add AAPL --us --1d', Colors.GREEN)}
  {color_text('ti add TSLA --us --1h', Colors.GREEN)}
  
  {color_text('# Analyze multiple stocks', Colors.GRAY)}
  {color_text('ti add 2330 0050 2454 --tw --1d', Colors.GREEN)}
  {color_text('ti add AAPL MSFT GOOGL --us --1d', Colors.GREEN)}
"""
    print(help_text)