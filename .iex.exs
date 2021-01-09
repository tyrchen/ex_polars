alias ExPolars.Series, as: S
alias ExPolars.DataFrame, as: DF
import Kernel, except: [+: 2, -: 2, *: 2, /: 2, ==: 2, <>: 2, >: 2, >=: 2, <: 2, <=: 2]
import S, only: [+: 2, -: 2, *: 2, /: 2, ==: 2, <>: 2, >: 2, >=: 2, <: 2, <=: 2]
