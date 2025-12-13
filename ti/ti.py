import argparse
from ti.services.stock_data_service import StockDataService
from ti.utils.colors import Colors, colorize
from ti.database.tables import create_tables, get_model_count, list_all_tables

def main():
    parser = argparse.ArgumentParser(description="技術指標計算與交易訊號分析工具",add_help=False)

    # 建立子命令
    subparsers = parser.add_subparsers(dest='command', help='可用命令')

    #幫助訊息
    help_parser = subparsers.add_parser('help', help='顯示幫助訊息')

    # add 子命令 - 計算技術指標並檢測k線型態
    add_parser = subparsers.add_parser('add', help='計算技術指標並檢測k線型態')

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
    add_parser.add_argument('--1wk', action='store_true', help='1 週數據')
    add_parser.add_argument('--1mo', action='store_true', help='1 月數據')
    add_parser.add_argument('--start', type=str, help='開始日期 (YYYY-MM-DD)')
    add_parser.add_argument('--end', type=str, help='結束日期 (YYYY-MM-DD)')

    # db 子命令 - 資料庫管理
    db_parser = subparsers.add_parser('db', help='資料庫管理')
    db_parser.add_argument('--init', action='store_true', help='初始化資料庫，建立所有資料表')
    db_parser.add_argument('--tables', action='store_true', help='列出當前資料庫的資料表')

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
        interval = None
        if args.__dict__.get('1m'):
            interval = '1m'
        elif args.__dict__.get('5m'):
            interval = '5m'
        elif args.__dict__.get('15m'):
            interval = '15m'
        elif args.__dict__.get('30m'):
            interval = '30m'
        elif args.__dict__.get('1h'):
            interval = '1h'
        elif args.__dict__.get('1d'):
            interval = '1d'
        elif args.__dict__.get('1wk'):
            interval = '1wk'
        elif args.__dict__.get('1mo'):
            interval = '1mo'
        else:
            print("請指定時間選項 (例: --1d, --1h)")
            return
        
        for symbol in args.symbols:
            try:
                if args.start and args.end:
                    print(f"正在處理 {symbol} ({market}, {interval})，日期範圍: {args.start} ~ {args.end}")
                    result = service.fetch_and_store_range(symbol, market, interval, args.start, args.end)
                    print(f"✓ {symbol} 技術指標資料已成功儲存")
                    print(f"  - 獲取了 {result['data_count']} 筆股票數據")
                    print(f"  - 計算了 {result['indicator_count']} 個技術指標")
                    print(f"  - 檢測了 {result['pattern_count']} 筆K線型態資料")
                    print(f"  - 數據已保存至資料表 {market}")
                else:    
                    print(f"正在處理 {symbol} ({market}, {interval})...")
                    result = service.fetch_and_store(symbol, market, interval)
                    print(f"✓ {symbol} 技術指標資料已成功儲存")
                    print(f"  - 獲取了 {result['data_count']} 筆股票數據")
                    print(f"  - 計算了 {result['indicator_count']} 個技術指標")
                    print(f"  - 檢測了 {result['pattern_count']} 筆K線型態資料")
                    print(f"  - 數據已保存至資料表 {market}")
                
            except Exception as e:
                print(f"✗ {symbol} 處理失敗: {str(e)}")
    
    # 處理 db 子命令 - 資料庫管理
    if args.command == 'db':
        if args.init:
            try:
                print("正在初始化資料庫...")
                create_tables()
                print(f"✓ 資料庫初始化成功")
                print(f"  已建立 {get_model_count()} 個市場資料表")
            except Exception as e:
                print(f"✗ 資料庫初始化失敗: {str(e)}")
        
        elif args.tables:
            try:
                tables = list_all_tables()
                
                if tables:
                    print(f"資料庫中的資料表 ({len(tables)} 個):")
                    for i, table in enumerate(tables, 1):
                        print(f"  {i}. {table}")
                else:
                    print("資料庫中沒有資料表，請先執行 'ti db --init' 初始化資料庫")
            except Exception as e:
                print(f"✗ 查詢資料表失敗: {str(e)}")
        
        else:
            print("請指定操作選項:")
            print("  --init    初始化資料庫，建立所有資料表")
            print("  --tables  列出當前資料庫的資料表")
        
def show_help():
    help_text = f"""
{colorize('Technical Indicators Analysis System', Colors.BOLD + Colors.CYAN)}

{colorize('Basic Usage:', Colors.BOLD + Colors.YELLOW)}
  {colorize('ti', Colors.GREEN)} {colorize('[command]', Colors.BLUE)} {colorize('[options]', Colors.MAGENTA)}

{colorize('Subcommands:', Colors.BOLD + Colors.YELLOW)}
  {colorize('ti add', Colors.GREEN)}                               Calculate technical indicators and analyze trading signals
  {colorize('ti db', Colors.GREEN)}                                Database configuration and management
{colorize('Technical Analysis:', Colors.BOLD + Colors.YELLOW)}
  {colorize('ti add', Colors.GREEN)} {colorize('<stock_symbol>', Colors.BLUE)} {colorize('--<market>', Colors.MAGENTA)} {colorize('--<interval>', Colors.MAGENTA)}   Analyze stock with technical indicators

{colorize('Market Options:', Colors.BOLD + Colors.YELLOW)}
  {colorize('--tw', Colors.MAGENTA)}          Taiwan Stock Exchange
  {colorize('--us', Colors.MAGENTA)}          US Stock Market
  {colorize('--etf', Colors.MAGENTA)}         ETF
  {colorize('--index', Colors.MAGENTA)}       Index
  {colorize('--crypto', Colors.MAGENTA)}      Cryptocurrency
  {colorize('--forex', Colors.MAGENTA)}       Foreign Exchange
  {colorize('--futures', Colors.MAGENTA)}     Futures
{colorize('Time Intervals:', Colors.BOLD + Colors.YELLOW)}
  {colorize('--1m', Colors.MAGENTA)}          1 minute data
  {colorize('--5m', Colors.MAGENTA)}          5 minutes data
  {colorize('--15m', Colors.MAGENTA)}         15 minutes data
  {colorize('--30m', Colors.MAGENTA)}         30 minutes data
  {colorize('--1h', Colors.MAGENTA)}          1 hour data
  {colorize('--1d', Colors.MAGENTA)}          1 day data
  {colorize('--1wk', Colors.MAGENTA)}         1 week data
  {colorize('--1mo', Colors.MAGENTA)}         1 month data

{colorize('Date Range Options:', Colors.BOLD + Colors.YELLOW)}
  {colorize('--start', Colors.MAGENTA)} {colorize('<date>', Colors.BLUE)}       Start date (YYYY-MM-DD format)
  {colorize('--end', Colors.MAGENTA)} {colorize('<date>', Colors.BLUE)}         End date (YYYY-MM-DD format)
{colorize('Database Management:', Colors.BOLD + Colors.YELLOW)}
  {colorize('ti db --init', Colors.GREEN)}                         Initialize database and create all tables
  {colorize('ti db --tables', Colors.GREEN)}                       List all database tables

{colorize('Usage Examples:', Colors.BOLD + Colors.YELLOW)}
  {colorize('# Initialize database', Colors.GRAY)}
  {colorize('ti db --init', Colors.GREEN)}
  
  {colorize('# Analyze Taiwan stocks', Colors.GRAY)}
  {colorize('ti add 2330 --tw --1d', Colors.GREEN)}
  {colorize('ti add 0050 --tw --1h', Colors.GREEN)}
  
  {colorize('# Analyze US stocks', Colors.GRAY)}
  {colorize('ti add AAPL --us --1d', Colors.GREEN)}
  {colorize('ti add TSLA --us --1h', Colors.GREEN)}
  
  {colorize('# Analyze multiple stocks', Colors.GRAY)}
  {colorize('ti add 2330 0050 2454 --tw --1d', Colors.GREEN)}
  {colorize('ti add AAPL MSFT GOOGL --us --1d', Colors.GREEN)}
  
  {colorize('# Analyze with date range', Colors.GRAY)}
  {colorize('ti add 2330 --tw --1d --start 2024-01-01 --end 2024-12-31', Colors.GREEN)}
  {colorize('ti add AAPL --us --1h --start 2024-06-01 --end 2024-06-30', Colors.GREEN)}
"""
    print(help_text)