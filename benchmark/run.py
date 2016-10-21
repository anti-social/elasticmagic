import sys
sys.path.insert(0, '.')

from bench import setup


def main():
    ap = setup()
    options = ap.parse_args()
    if not hasattr(options, 'action'):
        ap.print_help()
        return
    return options.action(options)


if __name__ == '__main__':
    main()
