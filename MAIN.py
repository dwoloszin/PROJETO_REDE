import Dump_3G
import GetLAC
import timeit


print ('\nprocessing GetLAC... ')
inicio = timeit.default_timer()
GetLAC.processArchive()
print ('GetLAC done!: ')
fim = timeit.default_timer()
print ('duracao: %f' % ((fim - inicio)/60) + ' min') 





print ('\nprocessing Dump_3G... ')
inicio = timeit.default_timer()
Dump_3G.processArchive()
print ('Dump_3G done!: ')
fim = timeit.default_timer()
print ('duracao: %f' % ((fim - inicio)/60) + ' min') 


